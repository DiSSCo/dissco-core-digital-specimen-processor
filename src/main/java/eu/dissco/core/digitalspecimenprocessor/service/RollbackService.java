package eu.dissco.core.digitalspecimenprocessor.service;

import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.nimbusds.jose.util.Pair;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaEventWithoutDOI;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaKey;
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
      Map<DigitalMediaKey, String> newMediaPidMap,
      boolean elasticRollback, boolean databseRollback) {
    // Rollback in database and/or in elastic
    updatedDigitalSpecimenRecords.forEach(
        updatedRecord -> rollbackUpdatedSpecimen(updatedRecord, elasticRollback, databseRollback));
    // Rollback handle records for those that need it
    filterUpdatesAndRollbackHandles(updatedDigitalSpecimenRecords);
    // Rollback the media pids created during this ingestion
    rollbackHandleCreation(newMediaPidMap.values().stream().toList());
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
      kafkaService.deadLetterEvent(
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
  public void rollbackNewSpecimens(Map<DigitalSpecimenRecord, Pair<List<String>,
      List<DigitalMediaEventWithoutDOI>>> digitalSpecimenRecords,
      Map<DigitalMediaKey, String> mediaPidMap,
      boolean elasticRollback, boolean databseRollback) {
    // Rollback in database and/or elastic
    digitalSpecimenRecords.forEach(
        (key, value) -> rollbackNewSpecimen(key, value, elasticRollback, databseRollback));
    // Rollback handle creation for specimen
    rollbackHandleCreation(
        digitalSpecimenRecords.keySet().stream().map(DigitalSpecimenRecord::id).toList());
    // Rollback handle creation for media
    rollbackHandleCreation(mediaPidMap.values().stream().toList());
  }

  private void rollbackNewSpecimen(DigitalSpecimenRecord digitalSpecimenRecord,
      Pair<List<String>, List<DigitalMediaEventWithoutDOI>> additionalInfo,
      boolean elasticRollback, boolean databaseRollback) {
    if (elasticRollback) {
      try {
        elasticRepository.rollbackSpecimen(digitalSpecimenRecord);
      } catch (IOException | ElasticsearchException e) {
        log.error("Fatal exception, unable to roll back: {}", digitalSpecimenRecord.id(), e);
      }
    }
    if (databaseRollback) {
      repository.rollbackSpecimen(digitalSpecimenRecord.id());
    }
    try {
      kafkaService.deadLetterEvent(
          new DigitalSpecimenEvent(additionalInfo.getLeft(),
              digitalSpecimenRecord.digitalSpecimenWrapper(),
              additionalInfo.getRight()));
    } catch (JsonProcessingException e) {
      log.error(DLQ_FAILED, digitalSpecimenRecord.id(), e);
    }
  }

  // Elastic failures

  public Map<DigitalSpecimenRecord, Pair<List<String>, List<DigitalMediaEventWithoutDOI>>> handlePartiallyFailedElasticInsert(
      Map<DigitalSpecimenRecord, Pair<List<String>, List<DigitalMediaEventWithoutDOI>>> digitalSpecimenRecords,
      BulkResponse bulkResponse) {
    var digitalSpecimenMap = digitalSpecimenRecords.keySet().stream()
        .collect(Collectors.toMap(DigitalSpecimenRecord::id, Function.identity()));
    ArrayList<DigitalSpecimenRecord> rollbackDigitalRecords = new ArrayList<>();
    bulkResponse.items().forEach(
        item -> {
          var digitalSpecimenRecord = digitalSpecimenMap.get(item.id());
          if (item.error() != null) {
            log.error("Failed item to insert into elastic search: {} with errors {}",
                digitalSpecimenRecord.id(), item.error().reason());
            rollbackDigitalRecords.add(digitalSpecimenRecord);
            rollbackNewSpecimen(digitalSpecimenRecord,
                digitalSpecimenRecords.get(
                    digitalSpecimenRecord), false, true);
            digitalSpecimenRecords.remove(digitalSpecimenRecord);
          } else {
            var successfullyPublished = publishEvents(digitalSpecimenRecord,
                digitalSpecimenRecords.get(digitalSpecimenRecord));
            if (!successfullyPublished) {
              rollbackDigitalRecords.add(digitalSpecimenRecord);
              digitalSpecimenRecords.remove(digitalSpecimenRecord);
            }
          }
        }
    );
    rollbackHandleCreation(rollbackDigitalRecords.stream().map(DigitalSpecimenRecord::id).toList());
    return digitalSpecimenRecords;
  }

  public Set<UpdatedDigitalSpecimenRecord> handlePartiallyFailedElasticUpdate(
      Set<UpdatedDigitalSpecimenRecord> digitalSpecimenRecords,
      BulkResponse bulkResponse) {

    var digitalSpecimenMap = digitalSpecimenRecords.stream()
        .collect(Collectors.toMap(
            updatedDigitalSpecimenRecord -> updatedDigitalSpecimenRecord.digitalSpecimenRecord()
                .id(), Function.identity()));
    List<DigitalSpecimenRecord> handleUpdatesToRollback = new ArrayList<>();
    bulkResponse.items().forEach(
        item -> {
          var digitalSpecimenRecord = digitalSpecimenMap.get(item.id());
          if (item.error() != null) {
            log.error("Failed item to insert into elastic search: {} with errors {}",
                digitalSpecimenRecord.digitalSpecimenRecord().id(), item.error().reason());
            handleUpdatesToRollback.add(digitalSpecimenRecord.currentDigitalSpecimen());
            rollbackUpdatedSpecimen(digitalSpecimenRecord, false, true);
            digitalSpecimenRecords.remove(digitalSpecimenRecord);
          } else {
            var successfullyPublished = publishUpdateEvent(digitalSpecimenRecord);
            if (!successfullyPublished) {
              handleUpdatesToRollback.add(digitalSpecimenRecord.currentDigitalSpecimen());
              digitalSpecimenRecords.remove(digitalSpecimenRecord);
            }
          }
        }
    );
    if (!handleUpdatesToRollback.isEmpty()) {
      rollbackHandleUpdate(handleUpdatesToRollback);
    }
    return digitalSpecimenRecords;
  }

  // Event publishing
  private boolean publishUpdateEvent(UpdatedDigitalSpecimenRecord updatedDigitalSpecimenRecord) {
    try {
      kafkaService.publishUpdateEvent(updatedDigitalSpecimenRecord.digitalSpecimenRecord(),
          updatedDigitalSpecimenRecord.jsonPatch());
      return true;
    } catch (JsonProcessingException e) {
      log.error("Failed to publish update event", e);
      return false;
    }
  }

  private boolean publishEvents(DigitalSpecimenRecord key,
      Pair<List<String>, List<DigitalMediaEventWithoutDOI>> additionalInfo) {
    try {
      kafkaService.publishCreateEvent(key);
    } catch (JsonProcessingException e) {
      log.error("Rolling back, failed to publish Create event", e);
      rollbackNewSpecimen(key, additionalInfo, true, true);
      return false;
    }
    additionalInfo.getLeft().forEach(aas -> {
      try {
        kafkaService.publishAnnotationRequestEvent(aas, key);
      } catch (JsonProcessingException e) {
        log.error(
            "No action taken, failed to publish annotation request event for aas: {} digital specimen: {}",
            aas, key.id(), e);
      }
    });
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
        kafkaService.deadLetterEvent(event);
      } catch (JsonProcessingException e2) {
        failedDlq.add(event);
      }
      if (!failedDlq.isEmpty()) {
        log.error("Critical error: Failed to DLQ the following events: {}", failedDlq);
      }
    }
  }

  private void rollbackHandleCreation(List<String> ids) {
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
