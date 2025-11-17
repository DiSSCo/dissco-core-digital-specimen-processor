package eu.dissco.core.digitalspecimenprocessor.service;

import static eu.dissco.core.digitalspecimenprocessor.domain.EntityRelationshipType.HAS_SPECIMEN;
import static eu.dissco.core.digitalspecimenprocessor.util.DigitalObjectUtils.DLQ_FAILED;
import static eu.dissco.core.digitalspecimenprocessor.util.DigitalObjectUtils.getIdForUri;
import static java.util.stream.Collectors.toSet;

import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.fge.jsonpatch.diff.JsonDiff;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaEvent;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaRecord;
import eu.dissco.core.digitalspecimenprocessor.domain.media.UpdatedDigitalMediaRecord;
import eu.dissco.core.digitalspecimenprocessor.domain.media.UpdatedDigitalMediaTuple;
import eu.dissco.core.digitalspecimenprocessor.domain.relation.DigitalMediaRelationshipTombstoneEvent;
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
import java.util.Date;
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
        .map(event -> mapToNewDigitalMediaRecord(event, pidMap))
        .collect(toSet());
    if (digitalMediaRecords.isEmpty()) {
      log.info("Mapped 0 events to their generated PIDs");
      return Collections.emptySet();
    }
    try {
      repository.createDigitalMediaRecord(digitalMediaRecords);
    } catch (DataAccessException e) {
      log.error("Database exception, unable to post new digital media to database", e);
      rollbackService.rollbackNewMedias(digitalMediaRecords, false, false);
      return Collections.emptySet();
    }
    log.info("{} digital media has been successfully committed to database",
        events.size());
    try {
      var bulkResponse = elasticRepository.indexDigitalMedia(digitalMediaRecords);
      if (!bulkResponse.errors()) {
        handleSuccessfulElasticInsert(digitalMediaRecords);
      } else {
        digitalMediaRecords = rollbackService.handlePartiallyFailedElasticInsertMedia(
            digitalMediaRecords,
            bulkResponse);
      }
      log.info("Successfully created {} new digital media", digitalMediaRecords.size());
      annotationPublisherService.publishAnnotationNewMedia(digitalMediaRecords);
      return digitalMediaRecords;
    } catch (IOException | ElasticsearchException e) {
      log.error("Rolling back, failed to insert records in elastic", e);
      rollbackService.rollbackNewMedias(digitalMediaRecords, false, true);
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
        log.error("Unable to update Handle record. Not proceeding with update.", e);
        updatedDigitalMediaTuples.stream().map(tuple -> {
          var attributes = tuple.digitalMediaEvent().digitalMediaWrapper().attributes();
          attributes.setId(tuple.currentDigitalMediaRecord().id());
          attributes.setDctermsIdentifier(tuple.currentDigitalMediaRecord().id());
          attributes.setOdsVersion(tuple.currentDigitalMediaRecord().version() + 1);
          attributes.setDctermsCreated(Date.from(tuple.currentDigitalMediaRecord().created()));
          return tuple.digitalMediaEvent();
        }).forEach(event -> {
          try {
            publisherService.deadLetterEventMedia(event);
          } catch (JsonProcessingException ex) {
            log.error(DLQ_FAILED, updatedDigitalMediaTuples, ex);
          }
        });
        return false;
      }
    }
    return true;
  }

  private void handleSuccessfulElasticUpdate(
      Set<UpdatedDigitalMediaRecord> updatedDigitalMediaRecords) {
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
      rollbackService.rollbackUpdatedMedias(updatedDigitalMediaRecords, true, true);
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
      Set<DigitalMediaRecord> digitalMediaRecords) {
    log.debug("Successfully indexed {} digital media", digitalMediaRecords);
    var recordsToRollback = new HashSet<DigitalMediaRecord>();
    digitalMediaRecords.forEach(digitalMediaRecord -> {
      if (!publishEvents(digitalMediaRecord)) {
        recordsToRollback.add(digitalMediaRecord);
      }
    });
    if (!recordsToRollback.isEmpty()) {
      recordsToRollback.forEach(digitalMediaRecords::remove);
      rollbackService.rollbackNewMedias(recordsToRollback, true, true);
    }
  }

  private boolean publishEvents(DigitalMediaRecord digitalMediaRecord) {
    try {
      publisherService.publishCreateEventMedia(digitalMediaRecord);
    } catch (JsonProcessingException e) {
      log.error("Rolling back, failed to publish Create event", e);
      return false;
    }
    return true;
  }

  public Set<DigitalMediaRecord> updateExistingDigitalMedia(
      List<UpdatedDigitalMediaTuple> updatedDigitalMediaTuples,
      boolean takeOverSpecimenRelationship) {
    var digitalMediaRecords = getUpdatedDigitalMediaRecords(updatedDigitalMediaTuples,
        takeOverSpecimenRelationship);
    var successfullyUpdatedHandles = updateHandles(updatedDigitalMediaTuples);
    if (!successfullyUpdatedHandles) {
      return Set.of();
    }
    log.info("Persisting to db");
    try {
      repository.createDigitalMediaRecord(
          digitalMediaRecords.stream().map(UpdatedDigitalMediaRecord::digitalMediaRecord)
              .collect(toSet()));
    } catch (DataAccessException e) {
      rollbackService.rollbackUpdatedMedias(digitalMediaRecords, false, false);
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
        handleSuccessfulElasticUpdate(digitalMediaRecords);
      } else {
        digitalMediaRecords = rollbackService.handlePartiallyFailedElasticUpdateMedia(
            digitalMediaRecords, bulkResponse
        );
        handleSuccessfulElasticUpdate(digitalMediaRecords);
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
      rollbackService.rollbackUpdatedMedias(digitalMediaRecords, false, true);
      return Set.of();
    }
  }

  private Set<UpdatedDigitalMediaRecord> getUpdatedDigitalMediaRecords(
      List<UpdatedDigitalMediaTuple> updatedDigitalMediaTuples,
      boolean takeOverSpecimenRelationship) {
    return updatedDigitalMediaTuples.stream()
        .map(tuple -> {
          var attributes = tuple.digitalMediaEvent().digitalMediaWrapper().attributes();
          setEntityRelationshipsForExistingMedia(attributes,
              tuple.currentDigitalMediaRecord().attributes(), tuple.newRelatedSpecimenDois(),
              takeOverSpecimenRelationship);
          return new UpdatedDigitalMediaRecord(
              new DigitalMediaRecord(
                  tuple.currentDigitalMediaRecord().id(),
                  tuple.currentDigitalMediaRecord().accessURI(),
                  tuple.currentDigitalMediaRecord().version() + 1,
                  tuple.currentDigitalMediaRecord().created(),
                  tuple.digitalMediaEvent().masList(),
                  attributes,
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
      DigitalMedia currentAttributes, Set<String> relatedDois,
      boolean takeOverSpecimenRelationship) {
    var er = new ArrayList<>(attributes.getOdsHasEntityRelationships());
    if (takeOverSpecimenRelationship) {
      var existingSpecimenErs = currentAttributes.getOdsHasEntityRelationships()
          .stream().filter(
              entityRelationship -> entityRelationship.getDwcRelationshipOfResource()
                  .equalsIgnoreCase(HAS_SPECIMEN.getRelationshipName())
          ).toList();
      er.addAll(existingSpecimenErs);
    }
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
      updatedDigitalSpecimenTuple.mediaRelationshipProcessResult()
          .tombstonedRelationships().stream().map(
              EntityRelationship::getOdsRelatedResourceURI)
          .map(uri -> new DigitalMediaRelationshipTombstoneEvent(
              updatedDigitalSpecimenTuple.currentSpecimen().id(), getIdForUri(uri))).forEach(
              event -> {
                try {
                  publisherService.publishDigitalMediaRelationTombstone(event);
                } catch (JsonProcessingException e) {
                  log.error("Failed to publish media relation tombstone event for event: {}",
                      event, e);
                }
              }
          );
    }
  }
}
