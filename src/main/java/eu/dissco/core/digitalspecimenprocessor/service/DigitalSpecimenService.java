package eu.dissco.core.digitalspecimenprocessor.service;

import static eu.dissco.core.digitalspecimenprocessor.domain.EntityRelationshipType.HAS_MEDIA;
import static eu.dissco.core.digitalspecimenprocessor.util.DigitalObjectUtils.DLQ_FAILED;

import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.fge.jsonpatch.diff.JsonDiff;
import eu.dissco.core.digitalspecimenprocessor.domain.relation.MediaRelationshipProcessResult;
import eu.dissco.core.digitalspecimenprocessor.domain.relation.PidProcessResult;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenEvent;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenRecord;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenWrapper;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.UpdatedDigitalSpecimenRecord;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.UpdatedDigitalSpecimenTuple;
import eu.dissco.core.digitalspecimenprocessor.exception.PidException;
import eu.dissco.core.digitalspecimenprocessor.repository.DigitalSpecimenRepository;
import eu.dissco.core.digitalspecimenprocessor.repository.ElasticSearchRepository;
import eu.dissco.core.digitalspecimenprocessor.schema.DigitalSpecimen;
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
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jooq.exception.DataAccessException;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class DigitalSpecimenService {

  private final DigitalSpecimenRepository repository;
  private final RollbackService rollbackService;
  private final ElasticSearchRepository elasticRepository;
  private final FdoRecordService fdoRecordService;
  private final RabbitMqPublisherService publisherService;
  private final HandleComponent handleComponent;
  private final AnnotationPublisherService annotationPublisherService;
  private final MidsService midsService;
  private final ObjectMapper mapper;
  private final DigitalMediaService digitalMediaService;

  public void updateEqualSpecimen(List<DigitalSpecimenRecord> currentDigitalMedia) {
    var currentIds = currentDigitalMedia.stream().map(DigitalSpecimenRecord::id).toList();
    repository.updateLastChecked(currentIds);
    log.info("Successfully updated lastChecked for {} existing digitalSpecimenWrapper",
        currentDigitalMedia.size());
  }

  public Set<DigitalSpecimenRecord> createNewDigitalSpecimen(List<DigitalSpecimenEvent> events,
      Map<String, PidProcessResult> pidMap) {
    var digitalSpecimenRecords = events.stream()
        .map(event -> mapToDigitalSpecimenRecord(event, pidMap))
        .filter(Objects::nonNull)
        .collect(Collectors.toSet());
    if (digitalSpecimenRecords.isEmpty()) {
      return Collections.emptySet();
    }
    log.info("Inserting {} new specimen into the database",
        digitalSpecimenRecords.size());
    try {
      repository.createDigitalSpecimenRecord(digitalSpecimenRecords);
    } catch (DataAccessException e) {
      log.error("Unable to insert new specimens into the database. Rolling back handles", e);
      rollbackService.rollbackNewSpecimens(events, pidMap, false, false);
      return Collections.emptySet();
    }
    try {
      log.info("Inserting {} new specimen into the elastic search",
          digitalSpecimenRecords.size());
      var bulkResponse = elasticRepository.indexDigitalSpecimen(
          digitalSpecimenRecords);
      if (!bulkResponse.errors()) {
        handleSuccessfulElasticInsert(digitalSpecimenRecords, events, pidMap);
      } else {
        digitalSpecimenRecords = rollbackService.handlePartiallyFailedElasticInsertSpecimen(
            digitalSpecimenRecords, bulkResponse, events);
      }
      log.info("Successfully created {} new digitalSpecimenRecord",
          digitalSpecimenRecords.size());
      annotationPublisherService.publishAnnotationNewSpecimen(
          digitalSpecimenRecords);
      return digitalSpecimenRecords;
    } catch (IOException | ElasticsearchException e) {
      log.error("Rolling back, failed to insert records in elastic", e);
      rollbackService.rollbackNewSpecimens(events, pidMap, false, true);
      return Collections.emptySet();
    }
  }

  public Set<DigitalSpecimenRecord> updateExistingDigitalSpecimen(
      List<UpdatedDigitalSpecimenTuple> updatedDigitalSpecimenTuples,
      Map<String, PidProcessResult> pidMap) {
    log.info("Persisting to Handle Server");
    var successfullyUpdatedHandles = updateHandles(updatedDigitalSpecimenTuples);
    if (!successfullyUpdatedHandles) {
      return Set.of();
    }
    var digitalSpecimenRecords = getUpdatedDigitalSpecimenRecords(updatedDigitalSpecimenTuples,
        pidMap);
    log.info("Persisting {} updated record to the database", digitalSpecimenRecords.size());
    try {
      repository.createDigitalSpecimenRecord(
          digitalSpecimenRecords.stream().map(UpdatedDigitalSpecimenRecord::digitalSpecimenRecord)
              .collect(Collectors.toSet()));
    } catch (DataAccessException e) {
      log.error("Unable to update records into database. Rolling back updates", e);
      rollbackService.rollbackUpdatedSpecimens(digitalSpecimenRecords, false, false);
      return Collections.emptySet();
    }
    log.info("Persisting {} updated records to elastic", digitalSpecimenRecords.size());
    try {
      var bulkResponse = elasticRepository.indexDigitalSpecimen(
          digitalSpecimenRecords.stream().map(UpdatedDigitalSpecimenRecord::digitalSpecimenRecord)
              .collect(Collectors.toSet()));
      if (!bulkResponse.errors()) {
        handleSuccessfulElasticUpdate(digitalSpecimenRecords);
      } else {
        digitalSpecimenRecords = rollbackService.handlePartiallyFailedElasticUpdateSpecimen(
            digitalSpecimenRecords, bulkResponse);
      }
      var successfullyProcessedRecords = digitalSpecimenRecords.stream()
          .map(UpdatedDigitalSpecimenRecord::digitalSpecimenRecord).collect(
              Collectors.toSet());
      log.info("Successfully updated {} digitalSpecimen records",
          successfullyProcessedRecords.size());
      annotationPublisherService.publishAnnotationUpdatedSpecimen(digitalSpecimenRecords);
      digitalMediaService.tombstoneSpecimenRelations(updatedDigitalSpecimenTuples);
      return successfullyProcessedRecords;
    } catch (IOException | ElasticsearchException e) {
      log.error("Rolling back, failed to insert records in elastic", e);
      rollbackService.rollbackUpdatedSpecimens(digitalSpecimenRecords, false, true);
      return Set.of();
    }
  }

  /* Elastic */

  private void handleSuccessfulElasticInsert(
      Set<DigitalSpecimenRecord> digitalSpecimenRecords, List<DigitalSpecimenEvent> events,
      Map<String, PidProcessResult> pidMap) {
    log.debug("Successfully indexed {} specimens", digitalSpecimenRecords);
    List<DigitalSpecimenRecord> rollbackRecords = new ArrayList<>();
    digitalSpecimenRecords.forEach(digitalSpecimenRecord -> {
      if (!publishCreateSpecimenEvents(digitalSpecimenRecord)) {
        rollbackRecords.add(digitalSpecimenRecord);
      }
    });
    if (!rollbackRecords.isEmpty()) {
      var rollbackRecordPhysId = rollbackRecords.stream().map(
          digitalSpecimenRecord -> digitalSpecimenRecord.digitalSpecimenWrapper()
              .physicalSpecimenID()).toList();
      var rollbackEvents = events.stream().filter(
          event -> rollbackRecordPhysId.contains(
              event.digitalSpecimenWrapper().physicalSpecimenID())
      ).toList();
      rollbackRecords.forEach(digitalSpecimenRecords::remove);
      rollbackService.rollbackNewSpecimens(rollbackEvents, pidMap, true, true);
    }
  }

  private void handleSuccessfulElasticUpdate(
      Set<UpdatedDigitalSpecimenRecord> digitalSpecimenRecords) {
    log.debug("Successfully indexed {} specimens", digitalSpecimenRecords);
    var failedRecords = new HashSet<UpdatedDigitalSpecimenRecord>();
    for (var digitalSpecimenRecord : digitalSpecimenRecords) {
      var successfullyPublished = publishUpdateEvent(digitalSpecimenRecord);
      if (!successfullyPublished) {
        failedRecords.add(digitalSpecimenRecord);
      }
    }
    if (!failedRecords.isEmpty()) {
      rollbackService.rollbackUpdatedSpecimens(failedRecords, true, true);
      failedRecords.forEach(digitalSpecimenRecords::remove);
    }
  }

  /* Queue Publishing */
  private boolean publishCreateSpecimenEvents(DigitalSpecimenRecord digitalSpecimenRecord) {
    try {
      publisherService.publishCreateEventSpecimen(digitalSpecimenRecord);
    } catch (JsonProcessingException e) {
      log.error("Rolling back, failed to publish Create event", e);
      return false;
    }
    return true;
  }

  private boolean publishUpdateEvent(UpdatedDigitalSpecimenRecord updatedDigitalSpecimenRecord) {
    try {
      publisherService.publishUpdateEventSpecimen(
          updatedDigitalSpecimenRecord.digitalSpecimenRecord(),
          updatedDigitalSpecimenRecord.jsonPatch());
      return true;
    } catch (JsonProcessingException e) {
      log.error("Failed to publish update event", e);
      return false;
    }
  }

  /* Entity Relationship */
  private DigitalSpecimenWrapper determineEntityRelationships(
      DigitalSpecimenWrapper digitalSpecimenWrapper, Map<String, PidProcessResult> pidMap,
      MediaRelationshipProcessResult mediaRelationshipProcessResult) {
    var mediaPids = pidMap.get(digitalSpecimenWrapper.physicalSpecimenID()).doisOfRelatedObjects();
    var existingMediaRelationshipPids = mediaRelationshipProcessResult.unchangedRelationships()
        .stream()
        .map(EntityRelationship::getDwcRelatedResourceID).toList();
    var newEntityRelationships = mediaPids
        .stream()
        .filter(pid -> !existingMediaRelationshipPids.contains(pid))
        .map(doi -> DigitalObjectUtils.buildEntityRelationship(HAS_MEDIA.getRelationshipName(),
            doi));
    var attributes = digitalSpecimenWrapper.attributes();
    var allRelationships = Stream.concat(newEntityRelationships,
            attributes.getOdsHasEntityRelationships().stream())
        .toList();
    allRelationships = removeTombstonedRelationships(mediaRelationshipProcessResult,
        allRelationships);
    if (allRelationships.stream().noneMatch(
        er -> er.getDwcRelationshipOfResource().equals(HAS_MEDIA.getRelationshipName()))) {
      attributes.setOdsIsKnownToContainMedia(Boolean.FALSE);
    } else {
      attributes.setOdsIsKnownToContainMedia(Boolean.TRUE);
    }
    attributes.setOdsHasEntityRelationships(allRelationships);
    return new DigitalSpecimenWrapper(
        digitalSpecimenWrapper.physicalSpecimenID(),
        digitalSpecimenWrapper.type(),
        attributes,
        digitalSpecimenWrapper.originalAttributes());
  }

  private List<EntityRelationship> removeTombstonedRelationships(
      MediaRelationshipProcessResult mediaRelationshipProcessResult,
      List<EntityRelationship> allRelationships) {
    return allRelationships.stream().filter(
        er -> !mediaRelationshipProcessResult.tombstonedRelationships().stream()
            .map(EntityRelationship::getOdsRelatedResourceURI).toList()
            .contains(er.getOdsRelatedResourceURI())).toList();
  }

  /* Transformations */

  private DigitalSpecimenRecord mapToDigitalSpecimenRecord(DigitalSpecimenEvent event,
      Map<String, PidProcessResult> pidMap) {
    var handle = pidMap.get(event.digitalSpecimenWrapper().physicalSpecimenID());
    if (handle == null) {
      try {
        log.error("handle not created for Digital Specimen {}",
            event.digitalSpecimenWrapper().physicalSpecimenID());
        publisherService.deadLetterEventSpecimen(event);
      } catch (JsonProcessingException e) {
        log.error("DLQ failed for specimen {}",
            event.digitalSpecimenWrapper().physicalSpecimenID());
      }
      return null;
    }
    return new DigitalSpecimenRecord(
        pidMap.get(event.digitalSpecimenWrapper().physicalSpecimenID()).doiOfTarget(),
        midsService.calculateMids(event.digitalSpecimenWrapper()),
        1,
        Instant.now(),
        determineEntityRelationships(event.digitalSpecimenWrapper(), pidMap,
            new MediaRelationshipProcessResult(List.of(), List.of(), List.of())),
        event.masList(), event.forceMasSchedule()
    );
  }

  // We remove the dcterms:modified from the comparison as it is generated and will always differ
  private JsonNode createJsonPatch(DigitalSpecimen currentDigitalSpecimen,
      DigitalSpecimen digitalSpecimen) {
    var jsonCurrentSpecimen = (ObjectNode) mapper.valueToTree(currentDigitalSpecimen);
    var jsonSpecimen = (ObjectNode) mapper.valueToTree(digitalSpecimen);
    jsonCurrentSpecimen.set("dcterms:modified", null);
    jsonSpecimen.set("dcterms:modified", null);
    return JsonDiff.asJson(jsonCurrentSpecimen, jsonSpecimen);
  }

  private Set<UpdatedDigitalSpecimenRecord> getUpdatedDigitalSpecimenRecords(
      List<UpdatedDigitalSpecimenTuple> updatedDigitalSpecimenTuples,
      Map<String, PidProcessResult> pidMap) {
    return updatedDigitalSpecimenTuples.stream().map(updateTuple -> {
          var digitalSpecimenRecord = new DigitalSpecimenRecord(
              updateTuple.currentSpecimen().id(),
              midsService.calculateMids(updateTuple.digitalSpecimenEvent().digitalSpecimenWrapper()),
              updateTuple.currentSpecimen().version() + 1,
              Instant.now(),
              determineEntityRelationships(
                  updateTuple.digitalSpecimenEvent().digitalSpecimenWrapper(), pidMap,
                  updateTuple.mediaRelationshipProcessResult()),
              updateTuple.digitalSpecimenEvent().masList(),
              updateTuple.digitalSpecimenEvent().forceMasSchedule());
          return new UpdatedDigitalSpecimenRecord(
              digitalSpecimenRecord,
              updateTuple.digitalSpecimenEvent().masList(),
              updateTuple.currentSpecimen(),
              createJsonPatch(
                  updateTuple.currentSpecimen().digitalSpecimenWrapper().attributes(),
                  updateTuple.digitalSpecimenEvent().digitalSpecimenWrapper().attributes()),
              updateTuple.digitalSpecimenEvent().digitalMediaEvents(),
              updateTuple.mediaRelationshipProcessResult());
        }
    ).collect(Collectors.toSet());
  }

  /* PID Records */

  private boolean updateHandles(List<UpdatedDigitalSpecimenTuple> updatedDigitalSpecimenTuples) {
    var digitalSpecimensToUpdate = updatedDigitalSpecimenTuples.stream()
        .filter(tuple -> fdoRecordService.handleNeedsUpdateSpecimen(
            tuple.currentSpecimen().digitalSpecimenWrapper(),
            tuple.digitalSpecimenEvent().digitalSpecimenWrapper()))
        .toList();

    if (!digitalSpecimensToUpdate.isEmpty()) {
      try {
        log.info("Updating {} handle records", digitalSpecimensToUpdate.size());
        var requests = fdoRecordService.buildUpdateHandleRequest(digitalSpecimensToUpdate);
        handleComponent.updateHandle(requests);
      } catch (PidException e) {
        log.error("Unable to update Handle record. Not proceeding with update. ", e);
        try {
          for (var tuple : updatedDigitalSpecimenTuples) {
            publisherService.deadLetterEventSpecimen(tuple.digitalSpecimenEvent());
          }
        } catch (JsonProcessingException jsonEx) {
          log.error(DLQ_FAILED, updatedDigitalSpecimenTuples, jsonEx);
        }
        return false;
      }
    }
    return true;
  }

}
