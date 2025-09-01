package eu.dissco.core.digitalspecimenprocessor.service;

import static eu.dissco.core.digitalspecimenprocessor.domain.EntityRelationshipType.HAS_SPECIMEN;
import static eu.dissco.core.digitalspecimenprocessor.util.DigitalObjectUtils.DLQ_FAILED;
import static eu.dissco.core.digitalspecimenprocessor.util.DigitalObjectUtils.DOI_PREFIX;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.fge.jsonpatch.diff.JsonDiff;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaEvent;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaRecord;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaWrapper;
import eu.dissco.core.digitalspecimenprocessor.domain.media.UpdatedDigitalMediaRecord;
import eu.dissco.core.digitalspecimenprocessor.domain.media.UpdatedDigitalMediaTuple;
import eu.dissco.core.digitalspecimenprocessor.domain.relation.PidProcessResult;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.UpdatedDigitalSpecimenTuple;
import eu.dissco.core.digitalspecimenprocessor.exception.PidException;
import eu.dissco.core.digitalspecimenprocessor.repository.DigitalMediaRepository;
import eu.dissco.core.digitalspecimenprocessor.repository.ElasticSearchRepository;
import eu.dissco.core.digitalspecimenprocessor.schema.DigitalMedia;
import eu.dissco.core.digitalspecimenprocessor.schema.EntityRelationship;
import eu.dissco.core.digitalspecimenprocessor.util.DigitalObjectUtils;
import eu.dissco.core.digitalspecimenprocessor.web.HandleComponent;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jooq.exception.DataAccessException;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class DigitalMediaService {

  private final DigitalMediaRepository repository;
  private final FdoRecordService fdoRecordService;
  private final HandleComponent handleComponent;
  private final ObjectMapper mapper;
  private final RollbackService rollbackService;
  private final ElasticSearchRepository elasticRepository;
  private final AnnotationPublisherService annotationPublisherService;
  private final RabbitMqPublisherService publisherService;

  public void updateEqualDigitalMedia(List<DigitalMediaRecord> currentDigitalMedia) {
    var currentIds = currentDigitalMedia.stream().map(DigitalMediaRecord::id).toList();
    repository.updateLastChecked(currentIds);
    log.info("Successfully updated lastChecked for {} existing digital media",
        currentIds.size());
  }

  public Set<DigitalMediaRecord> createNewDigitalMedia(
      List<DigitalMediaEvent> events, Map<String, PidProcessResult> pidMap) {
    var digitalMediaRecords = events.stream()
        .filter(
            event -> pidMap.containsKey(event.digitalMediaWrapper().attributes().getAcAccessURI()))
        .collect(toMap(
            event -> mapToNewDigitalMediaRecord(event, pidMap),
            DigitalMediaEvent::masList));
    if (digitalMediaRecords.isEmpty()) {
      log.info("Mapped 0 events to their generated PIDs");
      return Collections.emptySet();
    }
    try {
      repository.createDigitalMediaRecord(digitalMediaRecords.keySet());
    } catch (DataAccessException e) {
      log.error("Database exception, unable to post new digital media to database", e);
      rollbackService.rollbackNewMedias(events, pidMap, false, false);
      return Collections.emptySet();
    }
    log.info("{} digital media has been successfully committed to database",
        events.size());
    try {
      var bulkResponse = elasticRepository.indexDigitalMedia(digitalMediaRecords.keySet());
      if (!bulkResponse.errors()) {
        handleSuccessfulElasticInsert(digitalMediaRecords, events, pidMap);
      } else {
        digitalMediaRecords = rollbackService.handlePartiallyFailedElasticInsertMedia(
            digitalMediaRecords,
            bulkResponse, events);
      }
      log.info("Successfully created {} new digital media", digitalMediaRecords.size());
      annotationPublisherService.publishAnnotationNewMedia(digitalMediaRecords.keySet());
      return digitalMediaRecords.keySet();
    } catch (IOException | ElasticsearchException e) {
      log.error("Rolling back, failed to insert records in elastic", e);
      rollbackService.rollbackNewMedias(events, pidMap, false, true);
      return Collections.emptySet();
    }
  }

  private DigitalMediaRecord mapToNewDigitalMediaRecord(DigitalMediaEvent event,
      Map<String, PidProcessResult> pidMap) {
    var accessUri = event.digitalMediaWrapper().attributes().getAcAccessURI();
    var doi = pidMap.get(accessUri).doiOfTarget();
    var attributes = event.digitalMediaWrapper().attributes();
    setNewEntityRelationshipsForMedia(attributes, pidMap.get(accessUri).doisOfRelatedObjects());
    return new DigitalMediaRecord(
        doi,
        accessUri, 1, Instant.now(), event.masList(),
        event.digitalMediaWrapper().attributes(),
        event.digitalMediaWrapper().originalAttributes(), event.forceMasSchedule());
  }

  private boolean updateHandles(List<UpdatedDigitalMediaTuple> updatedDigitalMediaTuples) {
    var digitalMediasToUpdate = updatedDigitalMediaTuples.stream()
        .filter(tuple -> fdoRecordService.handleNeedsUpdateMedia(
            tuple.currentDigitalMediaRecord().attributes(),
            tuple.digitalMediaEvent().digitalMediaWrapper().attributes()))
        .toList();

    if (!digitalMediasToUpdate.isEmpty()) {
      try {
        log.info("Updating {} handle records", digitalMediasToUpdate.size());
        var requests = fdoRecordService.buildUpdateHandleRequestMedia(digitalMediasToUpdate);
        handleComponent.updateHandle(requests);
      } catch (PidException e) {
        log.error("Unable to update Handle record. Not proceeding with update. ", e);
        try {
          for (var tuple : updatedDigitalMediaTuples) {
            publisherService.deadLetterEventMedia(tuple.digitalMediaEvent());
          }
        } catch (JsonProcessingException jsonEx) {
          log.error(DLQ_FAILED, updatedDigitalMediaTuples, jsonEx);
        }
        return false;
      }
    }
    return true;
  }

  private void handleSuccessfulElasticUpdate(
      Set<UpdatedDigitalMediaRecord> updatedDigitalMediaRecords,
      List<DigitalMediaEvent> events, Map<String, PidProcessResult> pidMap) {
    log.debug("Successfully indexed {} digital media", updatedDigitalMediaRecords);
    var failedRecords = new HashSet<UpdatedDigitalMediaRecord>();
    for (var digitalMediaRecord : updatedDigitalMediaRecords) {
      var successfullyPublished = publishUpdateEvent(digitalMediaRecord);
      if (!successfullyPublished) {
        log.error("Digital media {} was not published in rabbitMQ",
            digitalMediaRecord.digitalMediaRecord().id());
        failedRecords.add(digitalMediaRecord);
      }
    }
    if (!failedRecords.isEmpty()) {
      log.info("Rolling back {} failed records", failedRecords.size());
      rollbackService.rollbackUpdatedMedias(updatedDigitalMediaRecords, true, true, events, pidMap);
      updatedDigitalMediaRecords.removeAll(failedRecords);
    }
  }

  private boolean publishUpdateEvent(UpdatedDigitalMediaRecord digitalMediaRecord) {
    try {
      publisherService.publishUpdateEventMedia(digitalMediaRecord.digitalMediaRecord(),
          digitalMediaRecord.jsonPatch());
      return true;
    } catch (JsonProcessingException e) {
      log.error("Rolling back, failed to publish update event", e);
      return false;
    }
  }

  private void handleSuccessfulElasticInsert(
      Map<DigitalMediaRecord, Set<String>> digitalMediaRecords, List<DigitalMediaEvent> events,
      Map<String, PidProcessResult> pidMap) {
    log.debug("Successfully indexed {} digital media", digitalMediaRecords);
    var recordsToRollback = new HashSet<DigitalMediaRecord>();
    digitalMediaRecords.forEach((digitalMediaRecord, enrichments) -> {
      if (!publishEvents(digitalMediaRecord, enrichments)) {
        recordsToRollback.add(digitalMediaRecord);
      }
    });
    if (!recordsToRollback.isEmpty()) {
      recordsToRollback.forEach(digitalMediaRecords::remove);
      var rollbackAccessUris = recordsToRollback.stream().map(
          DigitalMediaRecord::accessURI).toList();
      var rollbackEvents = events.stream().filter(
          event -> rollbackAccessUris.contains(
              event.digitalMediaWrapper().attributes().getAcAccessURI())
      ).toList();
      rollbackService.rollbackNewMedias(rollbackEvents, pidMap, true, true);
    }
  }

  private boolean publishEvents(DigitalMediaRecord digitalMediaRecord, Set<String> enrichments) {
    try {
      publisherService.publishCreateEventMedia(digitalMediaRecord);
    } catch (JsonProcessingException e) {
      log.error("Rolling back, failed to publish Create event", e);
      return false;
    }
    enrichments.forEach(mas -> {
      try {
        publisherService.publishAnnotationRequestEventMedia(mas, digitalMediaRecord);
      } catch (JsonProcessingException e) {
        log.error(
            "No action taken, failed to publish annotation request event for mas: {} digital media: {}",
            mas, digitalMediaRecord.id(), e);
      }
    });
    return true;
  }

  public Set<DigitalMediaRecord> updateExistingDigitalMedia(
      List<UpdatedDigitalMediaTuple> updatedDigitalMediaTuples,
      Map<String, PidProcessResult> pidMap) {
    var digitalMediaRecords = getUpdatedDigitalMediaRecords(updatedDigitalMediaTuples);
    var successfullyUpdatedHandles = updateHandles(updatedDigitalMediaTuples);
    var events = updatedDigitalMediaTuples.stream().map(
        UpdatedDigitalMediaTuple::digitalMediaEvent).toList();
    if (!successfullyUpdatedHandles) {
      return Set.of();
    }
    log.info("Persisting to db");
    try {
      repository.createDigitalMediaRecord(
          digitalMediaRecords.stream().map(UpdatedDigitalMediaRecord::digitalMediaRecord)
              .collect(toSet()));
    } catch (DataAccessException e) {
      rollbackService.rollbackUpdatedMedias(digitalMediaRecords, false, false, events, pidMap);
      log.error("Database exception: unable to post updates to db", e);
      return Collections.emptySet();
    }
    log.info("Persisting to elastic");
    try {
      var recordSet = digitalMediaRecords.stream()
          .map(UpdatedDigitalMediaRecord::digitalMediaRecord)
          .collect(toSet());
      var bulkResponse = elasticRepository.indexDigitalMedia(recordSet);
      if (!bulkResponse.errors()) {
        handleSuccessfulElasticUpdate(digitalMediaRecords, events, pidMap);
      } else {
        digitalMediaRecords = rollbackService.handlePartiallyFailedElasticUpdateMedia(
            digitalMediaRecords, bulkResponse,
            events, pidMap);
        handleSuccessfulElasticUpdate(digitalMediaRecords, events, pidMap);
      }
      var successfullyProcessedRecords = digitalMediaRecords.stream()
          .map(UpdatedDigitalMediaRecord::digitalMediaRecord).collect(
              toSet());
      log.info("Successfully updated {} digital media objects",
          successfullyProcessedRecords.size());
      annotationPublisherService.publishAnnotationUpdatedMedia(digitalMediaRecords);
      return successfullyProcessedRecords;
    } catch (IOException | ElasticsearchException e) {
      log.error("Rolling back, failed to insert records in elastic", e);
      rollbackService.rollbackUpdatedMedias(digitalMediaRecords, false, true, events, pidMap);
      return Set.of();
    }
  }

  private Set<UpdatedDigitalMediaRecord> getUpdatedDigitalMediaRecords(
      List<UpdatedDigitalMediaTuple> updatedDigitalMediaTuples) {
    return updatedDigitalMediaTuples.stream()
        .map(tuple -> {
          var attributes = tuple.digitalMediaEvent().digitalMediaWrapper().attributes();
          setEntityRelationshipsForExistingMedia(attributes,
              tuple.currentDigitalMediaRecord().attributes(), tuple.newRelatedSpecimenDois());
          return new UpdatedDigitalMediaRecord(
              new DigitalMediaRecord(
                  tuple.currentDigitalMediaRecord().id(),
                  tuple.currentDigitalMediaRecord().accessURI(),
                  tuple.currentDigitalMediaRecord().version() + 1,
                  tuple.currentDigitalMediaRecord().created(),
                  tuple.digitalMediaEvent().masList(), attributes,
                  tuple.digitalMediaEvent().digitalMediaWrapper().originalAttributes(),
                  tuple.digitalMediaEvent().forceMasSchedule()),
              tuple.digitalMediaEvent().masList(),
              tuple.currentDigitalMediaRecord(),
              createJsonPatch(tuple.currentDigitalMediaRecord().attributes(),
                  tuple.digitalMediaEvent().digitalMediaWrapper()
                      .attributes())
          );
        }).collect(toSet());
  }

  private void setEntityRelationshipsForExistingMedia(DigitalMedia attributes,
      DigitalMedia currentAttributes, Set<String> relatedDois) {
    var existingSpecimenErs = currentAttributes.getOdsHasEntityRelationships()
        .stream().filter(
            er -> er.getDwcRelationshipOfResource()
                .equalsIgnoreCase(HAS_SPECIMEN.getRelationshipName())
        ).toList();
    var er = new ArrayList<>(attributes.getOdsHasEntityRelationships());
    er.addAll(existingSpecimenErs);
    attributes.setOdsHasEntityRelationships(er);
    setNewEntityRelationshipsForMedia(attributes, relatedDois);
  }

  private void setNewEntityRelationshipsForMedia(DigitalMedia attributes, Set<String> relatedDois) {
    var specimenRelationships = relatedDois.stream().map(
        relatedDoi -> DigitalObjectUtils.buildEntityRelationship(HAS_SPECIMEN.getRelationshipName(),
            relatedDoi));
    attributes.setOdsHasEntityRelationships(
        Stream.concat(attributes.getOdsHasEntityRelationships().stream(),
            specimenRelationships).toList());
  }


  private JsonNode createJsonPatch(DigitalMedia currentDigitalMedia, DigitalMedia digitalMedia) {
    var jsonCurrentMedia = (ObjectNode) mapper.valueToTree(currentDigitalMedia);
    var jsonMedia = (ObjectNode) mapper.valueToTree(digitalMedia);
    jsonCurrentMedia.set("dcterms:modified", null);
    jsonMedia.set("dcterms:modified", null);
    return JsonDiff.asJson(jsonCurrentMedia, jsonMedia);
  }


  public void tombstoneSpecimenRelations(
      List<UpdatedDigitalSpecimenTuple> updatedDigitalSpecimenTuples) {
    for (var updatedDigitalSpecimenTuple : updatedDigitalSpecimenTuples) {
      var tombstonedDigitalSpecimenToDigitalMediaRelationship = updatedDigitalSpecimenTuple.mediaRelationshipProcessResult()
          .tombstonedRelationships().stream().map(
              EntityRelationship::getOdsRelatedResourceURI)
          .map(doi -> doi.toString().replace(DOI_PREFIX, "")).collect(toSet());
      var affectedMedia = repository.getExistingDigitalMediaByDoi(
          tombstonedDigitalSpecimenToDigitalMediaRelationship);
      for (var digitalMedia : affectedMedia) {
        var updatedEntityRelationships = digitalMedia.getOdsHasEntityRelationships().stream()
            .filter(er -> !tombstonedDigitalSpecimenToDigitalMediaRelationship.contains(
                er.getOdsRelatedResourceURI().toString().replace(DOI_PREFIX, ""))).toList();
        digitalMedia.setOdsHasEntityRelationships(updatedEntityRelationships);
        try {
          publisherService.republishMediaEvent(new DigitalMediaEvent(Collections.emptySet(), new DigitalMediaWrapper(digitalMedia.getOdsFdoType(), digitalMedia, mapper.createObjectNode()), false));
        } catch (JsonProcessingException e) {
          log.error("Failed to publish updated entity relationships for digital media: {}", digitalMedia.getId());
        }
      }

    }
  }
}
