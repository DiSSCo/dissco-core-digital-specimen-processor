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
import java.util.HashMap;
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
  private final RabbitMqPublisherService publisherService;
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
      publisherService.deadLetterEvent(
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
      publisherService.deadLetterEvent(
          new DigitalSpecimenEvent(additionalInfo.getLeft(),
              digitalSpecimenRecord.digitalSpecimenWrapper(),
              additionalInfo.getRight()));
    } catch (JsonProcessingException e) {
      log.error(DLQ_FAILED, digitalSpecimenRecord.id(), e);
    }
  }

  // Elastic Failures
  public Map<DigitalSpecimenRecord, Pair<List<String>, List<DigitalMediaEventWithoutDOI>>> handlePartiallyFailedElasticInsert(
      Map<DigitalSpecimenRecord, Pair<List<String>, List<DigitalMediaEventWithoutDOI>>> digitalSpecimenRecords,
      Map<DigitalMediaKey, String> mediaPidMap,
      BulkResponse bulkResponse) {
    var digitalSpecimenMap = digitalSpecimenRecords.keySet().stream()
        .collect(Collectors.toMap(DigitalSpecimenRecord::id, Function.identity()));
    var rollbackDigitalRecordIds = new ArrayList<String>();
    var mutableRecords = new HashMap<>(digitalSpecimenRecords);
    bulkResponse.items().forEach(
        item -> {
          var digitalSpecimenRecord = digitalSpecimenMap.get(item.id());
          if (item.error() != null) {
            log.error("Failed to insert item into elastic search: {} with errors {}",
                digitalSpecimenRecord.id(), item.error().reason());
            rollbackDigitalRecordIds.add(digitalSpecimenRecord.id());
            rollbackNewSpecimen(digitalSpecimenRecord, mutableRecords.get(
                digitalSpecimenRecord), false, true);
            rollbackDigitalRecordIds.addAll(
                getMediaPids(digitalSpecimenRecords, mediaPidMap, digitalSpecimenRecord));
            mutableRecords.remove(digitalSpecimenRecord);
          } else {
            var successfullyPublished = publishEvents(digitalSpecimenRecord,
                mutableRecords.get(digitalSpecimenRecord));
            if (!successfullyPublished) {
              rollbackDigitalRecordIds.add(digitalSpecimenRecord.id());
              mutableRecords.remove(digitalSpecimenRecord);
            }
          }
        }
    );
    rollbackHandleCreation(rollbackDigitalRecordIds);
    return mutableRecords;
  }

  public Set<UpdatedDigitalSpecimenRecord> handlePartiallyFailedElasticUpdate(
      Set<UpdatedDigitalSpecimenRecord> digitalSpecimenRecords,
      Map<DigitalMediaKey, String> mediaPidMap,
      BulkResponse bulkResponse) {
    var digitalSpecimenMap = digitalSpecimenRecords.stream()
        .collect(Collectors.toMap(
            updatedDigitalSpecimenRecord -> updatedDigitalSpecimenRecord.digitalSpecimenRecord()
                .id(), Function.identity()));
    var mutableDigitalSpecimenRecords = new HashSet<>(digitalSpecimenRecords);
    var handleUpdatesToRollback = new ArrayList<UpdatedDigitalSpecimenRecord>();
    var mediaPidsToRollback = new ArrayList<String>();

    bulkResponse.items().forEach(
        item -> {
          var digitalSpecimenRecord = digitalSpecimenMap.get(item.id());
          if (item.error() != null) {
            log.error("Failed to update item into elastic search: {} with errors {}",
                digitalSpecimenRecord.digitalSpecimenRecord().id(), item.error().reason());
            handleUpdatesToRollback.add(digitalSpecimenRecord);
            mediaPidsToRollback.addAll(
                getMediaPidsForUpdates(mutableDigitalSpecimenRecords, mediaPidMap,
                    digitalSpecimenRecord.digitalSpecimenRecord()));
            rollbackUpdatedSpecimen(digitalSpecimenRecord, false, true);
            mutableDigitalSpecimenRecords.remove(digitalSpecimenRecord);
          } else {
            var successfullyPublished = publishUpdateEvent(digitalSpecimenRecord);
            if (!successfullyPublished) {
              mediaPidsToRollback.addAll(getMediaPidsForUpdates(digitalSpecimenRecords, mediaPidMap,
                  digitalSpecimenRecord.digitalSpecimenRecord()));
              handleUpdatesToRollback.add(digitalSpecimenRecord);
              mutableDigitalSpecimenRecords.remove(digitalSpecimenRecord);
            }
          }
        }
    );
    filterUpdatesAndRollbackHandles(handleUpdatesToRollback);
    rollbackHandleCreation(mediaPidsToRollback);
    return mutableDigitalSpecimenRecords;
  }

  private static List<String> getMediaPidsForUpdates(
      Set<UpdatedDigitalSpecimenRecord> digitalSpecimenRecords,
      Map<DigitalMediaKey, String> mediaPidMap, DigitalSpecimenRecord digitalSpecimenRecord) {
    return digitalSpecimenRecords.stream()
        .filter(
            specimen -> specimen.digitalSpecimenRecord().id().equals(digitalSpecimenRecord.id()))
        .map(UpdatedDigitalSpecimenRecord::digitalMediaObjectEvents)
        .flatMap(List::stream)
        .map(mediaEvent -> mediaPidMap.get(new DigitalMediaKey(digitalSpecimenRecord.id(),
            mediaEvent.digitalMediaObjectWithoutDoi().attributes().getAcAccessURI())))
        .toList();
  }

  private static List<String> getMediaPids(
      Map<DigitalSpecimenRecord, Pair<List<String>, List<DigitalMediaEventWithoutDOI>>> digitalSpecimenRecords,
      Map<DigitalMediaKey, String> mediaPidMap, DigitalSpecimenRecord digitalSpecimenRecord) {
    return digitalSpecimenRecords.get(digitalSpecimenRecord).getRight().stream()
        .map(mediaEvent -> mediaEvent.digitalMediaObjectWithoutDoi().attributes().getAcAccessURI())
        .map(mediaUri -> mediaPidMap.get(new DigitalMediaKey(digitalSpecimenRecord.id(), mediaUri)))
        .toList();
  }

  // Event publishing
  private boolean publishUpdateEvent(UpdatedDigitalSpecimenRecord updatedDigitalSpecimenRecord) {
    try {
      publisherService.publishUpdateEvent(updatedDigitalSpecimenRecord.digitalSpecimenRecord(),
          updatedDigitalSpecimenRecord.jsonPatch());
      return true;
    } catch (JsonProcessingException e) {
      log.error("Failed to publish update event", e);
      rollbackUpdatedSpecimen(updatedDigitalSpecimenRecord, true, true);
      return false;
    }
  }

  private boolean publishEvents(DigitalSpecimenRecord key,
      Pair<List<String>, List<DigitalMediaEventWithoutDOI>> additionalInfo) {
    try {
      publisherService.publishCreateEvent(key);
    } catch (JsonProcessingException e) {
      log.error("Rolling back, failed to publish Create event", e);
      rollbackNewSpecimen(key, additionalInfo, true, true);
      return false;
    }
    additionalInfo.getLeft().forEach(aas -> {
      try {
        publisherService.publishAnnotationRequestEvent(aas, key);
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
        publisherService.deadLetterEvent(event);
      } catch (JsonProcessingException e2) {
        failedDlq.add(event);
      }
      if (!failedDlq.isEmpty()) {
        log.error("Critical error: Failed to DLQ the following events: {}", failedDlq);
      }
    }
  }

  private void rollbackHandleCreation(List<String> ids) {
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
