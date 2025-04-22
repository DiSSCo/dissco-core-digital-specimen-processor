package eu.dissco.core.digitalspecimenprocessor.service;

import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.nimbusds.jose.util.Pair;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaKey;
import eu.dissco.core.digitalspecimenprocessor.domain.relation.PidProcessResult;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenEvent;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenRecord;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenWrapper;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.UpdatedDigitalSpecimenRecord;
import eu.dissco.core.digitalspecimenprocessor.exception.PidException;
import eu.dissco.core.digitalspecimenprocessor.repository.DigitalSpecimenRepository;
import eu.dissco.core.digitalspecimenprocessor.repository.ElasticSearchRepository;
import eu.dissco.core.digitalspecimenprocessor.web.HandleComponent;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jooq.exception.DataAccessException;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class RollbackService {

  private final ElasticSearchRepository elasticRepository;
  private final KafkaPublisherService kafkaService;
  private final DigitalSpecimenRepository repository;
  private final FdoRecordService fdoRecordService;
  private final HandleComponent handleComponent;

  private static final String DLQ_FAILED = "Fatal exception, unable to dead letter queue: {}";

  // Rollback updated specimen

  public void rollbackUpdatedSpecimens(
      Collection<UpdatedDigitalSpecimenRecord> updatedDigitalSpecimenRecords,
      boolean elasticRollback, boolean databseRollback) {
    // Rollback in database and/or in elastic
    updatedDigitalSpecimenRecords.forEach(
        updatedRecord -> rollbackUpdatedSpecimen(updatedRecord, elasticRollback, databseRollback));
    // Rollback handle records for those that need it
    filterUpdatesAndRollbackHandles(updatedDigitalSpecimenRecords);
  }

  private void rollbackUpdatedSpecimen(UpdatedDigitalSpecimenRecord updatedDigitalSpecimenRecord,
      boolean elasticRollback, boolean databaseRollback) {
    if (elasticRollback) {
      try {
        elasticRepository.rollbackVersion(updatedDigitalSpecimenRecord.currentDigitalSpecimen());
      } catch (IOException | ElasticsearchException e) {
        log.error("Fatal exception, unable to roll back update for: "
            + updatedDigitalSpecimenRecord.currentDigitalSpecimen(), e);
      }
    }
    if (databaseRollback) {
      rollBackToEarlierDatabaseVersion(updatedDigitalSpecimenRecord.currentDigitalSpecimen());
    }
    try {
      kafkaService.deadLetterEventSpecimen(
          new DigitalSpecimenEvent(updatedDigitalSpecimenRecord.enrichment(),
              updatedDigitalSpecimenRecord.digitalSpecimenRecord()
                  .digitalSpecimenWrapper(),
              updatedDigitalSpecimenRecord.digitalMediaObjectEvents()));
    } catch (JsonProcessingException e) {
      log.error(DLQ_FAILED, updatedDigitalSpecimenRecord.digitalSpecimenRecord().id(), e);
    }
  }

  private void rollBackToEarlierDatabaseVersion(DigitalSpecimenRecord currentDigitalSpecimen) {
    try {
      repository.createDigitalSpecimenRecord(List.of(currentDigitalSpecimen));
    } catch (DataAccessException e) {
      log.error("Unable to rollback to previous version");
    }
  }

  // Rollback New Specimen
  public void rollbackNewSpecimens(List<DigitalSpecimenEvent> events,
      Map<String, PidProcessResult> pidMap,
      boolean elasticRollback, boolean databaseRollback) {
    var rollbackMap = createRollbackMap(events, pidMap);
    // Rollback in database and/or elastic
    rollbackMap.forEach(
        (key, value) -> rollbackNewSpecimen(key, value, elasticRollback, databaseRollback));
    // Rollback handle creation for specimen
    rollbackHandleCreation(rollbackMap.keySet());
  }

  public void rollbackNewSpecimensSubset(List<DigitalSpecimenRecord> digitalSpecimenRecords,
      List<DigitalSpecimenEvent> events, Map<String, PidProcessResult> pidMap,
      boolean elasticRollback, boolean databaseRollback) {
    var rollbackRecordPhysId = digitalSpecimenRecords.stream().map(
        digitalSpecimenRecord -> digitalSpecimenRecord.digitalSpecimenWrapper()
            .physicalSpecimenID()).toList();
    var rollbackEvents = events.stream().filter(
        event -> rollbackRecordPhysId.contains(event.digitalSpecimenWrapper().physicalSpecimenID())
    ).toList();
    rollbackNewSpecimens(rollbackEvents, pidMap, elasticRollback, databaseRollback);
  }

  private static Map<String, DigitalSpecimenEvent> createRollbackMap(
      List<DigitalSpecimenEvent> events, Map<String, PidProcessResult> pidMap) {
    return events.stream().collect(Collectors.toMap(
        event -> pidMap.get(event.digitalSpecimenWrapper().physicalSpecimenID()).doi(),
        event -> event));
  }

  private void rollbackNewSpecimen(String id, DigitalSpecimenEvent event,
      boolean elasticRollback, boolean databaseRollback) {
    if (elasticRollback) {
      try {
        elasticRepository.rollbackSpecimen(id);
      } catch (IOException | ElasticsearchException e) {
        log.error("Fatal exception, unable to roll back: {}", id, e);
      }
    }
    if (databaseRollback) {
      repository.rollbackSpecimen(id);
    }
    try {
      kafkaService.deadLetterEventSpecimen(event);
    } catch (JsonProcessingException e) {
      log.error(DLQ_FAILED, id, e);
    }
  }

  // Elastic Failures
  public Set<DigitalSpecimenRecord> handlePartiallyFailedElasticInsert(
      Set<DigitalSpecimenRecord> digitalSpecimenRecords,
      BulkResponse bulkResponse, List<DigitalSpecimenEvent> events) {
    var digitalSpecimenMap = digitalSpecimenRecords.stream()
        .collect(Collectors.toMap(
            DigitalSpecimenRecord::id,
            digitalSpecimenRecord -> Pair.of(digitalSpecimenRecord, getDigitalSpecimenEvent(events,
                digitalSpecimenRecord.digitalSpecimenWrapper().physicalSpecimenID())
            )));
    var rollbackDigitalRecordIds = new ArrayList<String>();
    bulkResponse.items().forEach(
        item -> {
          var digitalSpecimenRecord = digitalSpecimenMap.get(item.id()).getLeft();
          var event = digitalSpecimenMap.get(item.id()).getRight();
          if (item.error() != null) {
            log.error("Failed to insert item into elastic search: {} with errors {}",
                item.id(), item.error().reason());
            rollbackDigitalRecordIds.add(item.id());
            rollbackNewSpecimen(item.id(), event, false, true);
          } else {
            var successfullyPublished = publishEvents(digitalSpecimenRecord, event);
            if (!successfullyPublished) {
              rollbackDigitalRecordIds.add(digitalSpecimenRecord.id());
            }
          }
        }
    );
    rollbackHandleCreation(rollbackDigitalRecordIds);
    return new HashSet<>(digitalSpecimenRecords).stream()
        .filter(r -> rollbackDigitalRecordIds.contains(r.id())).collect(
            Collectors.toSet());
  }

  private static DigitalSpecimenEvent getDigitalSpecimenEvent(
      List<DigitalSpecimenEvent> digitalSpecimenEvents, String physId) {
    for (var digitalSpecimenEvent : digitalSpecimenEvents) {
      if (physId.equalsIgnoreCase(
          digitalSpecimenEvent.digitalSpecimenWrapper().physicalSpecimenID())) {
        return digitalSpecimenEvent;
      }
    }
    throw new IllegalStateException();
  }

  public Set<UpdatedDigitalSpecimenRecord> handlePartiallyFailedElasticUpdate(
      Set<UpdatedDigitalSpecimenRecord> digitalSpecimenRecords,
      BulkResponse bulkResponse) {
    var digitalSpecimenMap = digitalSpecimenRecords.stream()
        .collect(Collectors.toMap(
            updatedDigitalSpecimenRecord -> updatedDigitalSpecimenRecord.digitalSpecimenRecord()
                .id(), Function.identity()));
    var mutableDigitalSpecimenRecords = new HashSet<>(digitalSpecimenRecords);
    var handleUpdatesToRollback = new ArrayList<UpdatedDigitalSpecimenRecord>();
    bulkResponse.items().forEach(
        item -> {
          var digitalSpecimenRecord = digitalSpecimenMap.get(item.id());
          if (item.error() != null) {
            log.error("Failed to update item into elastic search: {} with errors {}",
                digitalSpecimenRecord.digitalSpecimenRecord().id(), item.error().reason());
            handleUpdatesToRollback.add(digitalSpecimenRecord);
            rollbackUpdatedSpecimen(digitalSpecimenRecord, false, true);
            mutableDigitalSpecimenRecords.remove(digitalSpecimenRecord);
          } else {
            var successfullyPublished = publishUpdateEvent(digitalSpecimenRecord);
            if (!successfullyPublished) {
              handleUpdatesToRollback.add(digitalSpecimenRecord);
              mutableDigitalSpecimenRecords.remove(digitalSpecimenRecord);
            }
          }
        }
    );
    filterUpdatesAndRollbackHandles(handleUpdatesToRollback);
    return mutableDigitalSpecimenRecords;
  }

  // Event publishing
  private boolean publishUpdateEvent(UpdatedDigitalSpecimenRecord updatedDigitalSpecimenRecord) {
    try {
      kafkaService.publishUpdateEvent(updatedDigitalSpecimenRecord.digitalSpecimenRecord(),
          updatedDigitalSpecimenRecord.jsonPatch());
      return true;
    } catch (JsonProcessingException e) {
      log.error("Failed to publish update event", e);
      rollbackUpdatedSpecimen(updatedDigitalSpecimenRecord, true, true);
      return false;
    }
  }

  private boolean publishEvents(DigitalSpecimenRecord digitalSpecimenRecord,
      DigitalSpecimenEvent event) {
    //Pair<List<String>, List<DigitalMediaEventWithoutDOI>> additionalInfo) {
    try {
      kafkaService.publishCreateEvent(digitalSpecimenRecord);
    } catch (JsonProcessingException e) {
      log.error("Rolling back, failed to publish Create event", e);
      rollbackNewSpecimen(digitalSpecimenRecord.id(), event, true, true);
      return false;
    }
    /*
    additionalInfo.getLeft().forEach(aas -> {
      try {
        kafkaService.publishAnnotationRequestEvent(aas, digitalSpecimenRecord);
      } catch (JsonProcessingException e) {
        log.error(
            "No action taken, failed to publish annotation request event for aas: {} digital specimen: {}",
            aas, digitalSpecimenRecord.id(), e);
      }
    });*/
    return true;
  }

  // Rollback Handle
  public void pidCreationFailed(List<DigitalSpecimenEvent> events) {
    try {
      handleComponent.rollbackFromPhysId(events.stream()
          .map(DigitalSpecimenEvent::digitalSpecimenWrapper)
          .map(DigitalSpecimenWrapper::physicalSpecimenID).toList());
    } catch (PidException e) {
      log.error("Unable to roll back PIDs", e);
    }
    List<DigitalSpecimenEvent> failedDlq = new ArrayList<>();
    for (var event : events) {
      try {
        kafkaService.deadLetterEventSpecimen(event);
      } catch (JsonProcessingException e2) {
        failedDlq.add(event);
      }
      if (!failedDlq.isEmpty()) {
        log.error("Critical error: Failed to DLQ the following events: {}", failedDlq);
      }
    }
  }

  private void rollbackHandleCreation(Collection<String> ids) {
    if (ids.isEmpty()) {
      return;
    }
    try {
      handleComponent.rollbackHandleCreation(ids);
    } catch (PidException e) {
      log.error("Unable to rollback new handles: {}", ids, e);
    }
  }

  // Only rollback the handle updates for records that were updated
  private void filterUpdatesAndRollbackHandles(Collection<UpdatedDigitalSpecimenRecord> records) {
    var recordsToRollback = records.stream()
        .filter(
            r -> fdoRecordService.handleNeedsUpdate(
                r.currentDigitalSpecimen().digitalSpecimenWrapper(),
                r.digitalSpecimenRecord().digitalSpecimenWrapper()))
        .map(UpdatedDigitalSpecimenRecord::currentDigitalSpecimen)
        .toList();
    rollbackHandleUpdate(recordsToRollback);
  }

  private void rollbackHandleUpdate(List<DigitalSpecimenRecord> recordsToRollback) {
    if (recordsToRollback.isEmpty()) {
      return;
    }
    try {
      var request = fdoRecordService.buildRollbackUpdateRequest(recordsToRollback);
      handleComponent.rollbackHandleUpdate(request);
    } catch (PidException e) {
      var ids = recordsToRollback.stream().map(DigitalSpecimenRecord::id).toList();
      log.error(
          "Unable to rollback handles for Updated specimens. Bad handles: {}. Revert handles to the following records: {}",
          ids, recordsToRollback);
    }
  }


}
