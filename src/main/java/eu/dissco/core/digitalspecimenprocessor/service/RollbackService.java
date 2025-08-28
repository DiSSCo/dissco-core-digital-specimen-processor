package eu.dissco.core.digitalspecimenprocessor.service;

import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaEvent;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaRecord;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaWrapper;
import eu.dissco.core.digitalspecimenprocessor.domain.media.UpdatedDigitalMediaRecord;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenEvent;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenRecord;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.UpdatedDigitalSpecimenRecord;
import eu.dissco.core.digitalspecimenprocessor.exception.PidException;
import eu.dissco.core.digitalspecimenprocessor.repository.DigitalMediaRepository;
import eu.dissco.core.digitalspecimenprocessor.repository.DigitalSpecimenRepository;
import eu.dissco.core.digitalspecimenprocessor.repository.ElasticSearchRepository;
import eu.dissco.core.digitalspecimenprocessor.web.HandleComponent;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
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
  private final DigitalSpecimenRepository specimenRepository;
  private final DigitalMediaRepository mediaRepository;
  private final FdoRecordService fdoRecordService;
  private final HandleComponent handleComponent;

  private static final String DLQ_FAILED = "Fatal exception, unable to dead letter queue: {}";

  // Rollback updated specimen

  public void rollbackUpdatedSpecimens(
      Set<UpdatedDigitalSpecimenRecord> updatedDigitalSpecimenRecords,
      boolean elasticRollback, boolean databseRollback) {
    // Rollback in database and/or in elastic
    updatedDigitalSpecimenRecords.forEach(
        updatedRecord -> rollbackUpdatedSpecimen(updatedRecord, elasticRollback, databseRollback));
    // Rollback handle records for those that need it
    filterUpdatesAndRollbackHandlesSpecimen(updatedDigitalSpecimenRecords);
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
      rollBackToEarlierDatabaseVersionSpecimen(
          updatedDigitalSpecimenRecord.currentDigitalSpecimen());
    }
    try {
      publisherService.deadLetterEventSpecimen(
          specimenEventFromRecord(updatedDigitalSpecimenRecord.digitalSpecimenRecord()));
    } catch (JsonProcessingException e) {
      log.error(DLQ_FAILED, updatedDigitalSpecimenRecord.digitalSpecimenRecord().id(), e);
    }
  }

  private void rollBackToEarlierDatabaseVersionSpecimen(
      DigitalSpecimenRecord currentDigitalSpecimen) {
    try {
      specimenRepository.createDigitalSpecimenRecord(Set.of(currentDigitalSpecimen));
    } catch (DataAccessException e) {
      log.error("Unable to rollback specimen {} to previous version", currentDigitalSpecimen.id());
    }
  }

  private void rollBackToEarlierDatabaseVersionMedia(DigitalMediaRecord currentDigitalMedia) {
    try {
      mediaRepository.createDigitalMediaRecord(Set.of(currentDigitalMedia));
    } catch (DataAccessException e) {
      log.error("Unable to rollback media {} to previous version", currentDigitalMedia.id());
    }
  }

  // Rollback Updated Media

  public void rollbackUpdatedMedias(Set<UpdatedDigitalMediaRecord> updatedDigitalMediaRecords,
      boolean elasticRollback, boolean databseRollback) {
    updatedDigitalMediaRecords.forEach(
        updatedRecord -> rollbackUpdatedMedia(updatedRecord, elasticRollback, databseRollback));
    // Rollback handle records for those that need it
    filterUpdatesAndRollbackHandlesMedia(updatedDigitalMediaRecords);
  }

  private void rollbackUpdatedMedia(UpdatedDigitalMediaRecord updatedDigitalMediaRecord,
      boolean elasticRollback, boolean databaseRollback) {
    if (elasticRollback) {
      try {
        elasticRepository.rollbackVersion(updatedDigitalMediaRecord.currentDigitalMediaRecord());
      } catch (IOException | ElasticsearchException e) {
        log.error("Fatal exception, unable to roll back update for: "
            + updatedDigitalMediaRecord.currentDigitalMediaRecord(), e);
      }
    }
    if (databaseRollback) {
      rollBackToEarlierDatabaseVersionMedia(updatedDigitalMediaRecord.currentDigitalMediaRecord());
    }
    try {
      publisherService.deadLetterEventMedia(
          mediaEventFromRecord(updatedDigitalMediaRecord.digitalMediaRecord()));
    } catch (JsonProcessingException e) {
      log.error(DLQ_FAILED, updatedDigitalMediaRecord.digitalMediaRecord().id(), e);
    }
  }

  // Rollback New Specimen
  public void rollbackNewSpecimens(Set<DigitalSpecimenRecord> digitalSpecimenRecords,
      boolean elasticRollback, boolean databaseRollback) {
    // Rollback in database and/or elastic
    digitalSpecimenRecords.forEach(
        digitalSpecimenRecord -> rollbackNewSpecimen(digitalSpecimenRecord, elasticRollback,
            databaseRollback));
    // Rollback handle creation for specimen
  }

  private void rollbackNewSpecimen(DigitalSpecimenRecord digitalSpecimenRecord,
      boolean elasticRollback, boolean databaseRollback) {
    if (elasticRollback) {
      try {
        elasticRepository.rollbackObject(digitalSpecimenRecord.id(), true);
      } catch (IOException | ElasticsearchException e) {
        log.error("Fatal exception, unable to roll back: {}", digitalSpecimenRecord.id(), e);
      }
    }
    if (databaseRollback) {
      specimenRepository.rollbackSpecimen(digitalSpecimenRecord.id());
    }
    try {
      publisherService.deadLetterEventSpecimen(specimenEventFromRecord(digitalSpecimenRecord));
    } catch (JsonProcessingException e) {
      log.error(DLQ_FAILED, digitalSpecimenRecord.id(), e);
    }
  }

  // Rollback New Media
  public void rollbackNewMedias(Set<DigitalMediaRecord> digitalMediaRecords,
      boolean elasticRollback, boolean databaseRollback) {
    // Rollback in database and/or elastic
    digitalMediaRecords.forEach(
        digitalMediaRecord -> rollbackNewMedia(digitalMediaRecord, elasticRollback,
            databaseRollback));
  }

  private void rollbackNewMedia(DigitalMediaRecord digitalMediaRecord,
      boolean elasticRollback, boolean databaseRollback) {
    if (elasticRollback) {
      try {
        elasticRepository.rollbackObject(digitalMediaRecord.id(), false);
      } catch (IOException | ElasticsearchException e) {
        log.error("Fatal exception, unable to roll back: {}", digitalMediaRecord.id(), e);
      }
    }
    if (databaseRollback) {
      mediaRepository.rollBackDigitalMedia(digitalMediaRecord.id());
    }
    try {
      publisherService.deadLetterEventMedia(mediaEventFromRecord(digitalMediaRecord));
    } catch (JsonProcessingException e) {
      log.error(DLQ_FAILED, digitalMediaRecord.id(), e);
    }
  }

  private static DigitalMediaEvent mediaEventFromRecord(DigitalMediaRecord digitalMediaRecord) {
    return new DigitalMediaEvent(
        digitalMediaRecord.masIds(),
        new DigitalMediaWrapper(
            digitalMediaRecord.attributes().getType(),
            digitalMediaRecord.attributes(),
            digitalMediaRecord.originalAttributes()
        ),
        digitalMediaRecord.forceMasSchedule()
    );
  }

  private static DigitalSpecimenEvent specimenEventFromRecord(
      DigitalSpecimenRecord digitalSpecimenRecord) {
    return new DigitalSpecimenEvent(
        digitalSpecimenRecord.masIds(),
        digitalSpecimenRecord.digitalSpecimenWrapper(),
        digitalSpecimenRecord.digitalMediaEvents(),
        digitalSpecimenRecord.forceMasSchedule()
    );
  }

  // Elastic Failures
  public Set<DigitalSpecimenRecord> handlePartiallyFailedElasticInsertSpecimen(
      Set<DigitalSpecimenRecord> digitalSpecimenRecords,
      BulkResponse bulkResponse) {
    var digitalSpecimenMap = digitalSpecimenRecords.stream()
        .collect(Collectors.toMap(
            DigitalSpecimenRecord::id, Function.identity()));
    var rollbackDigitalRecordIds = new HashSet<String>();
    bulkResponse.items().forEach(
        item -> {
          var digitalSpecimenRecord = digitalSpecimenMap.get(item.id());
          if (item.error() != null) {
            log.error("Failed to insert item into elastic search: {} with errors {}",
                item.id(), item.error().reason());
            rollbackDigitalRecordIds.add(item.id());
            rollbackNewSpecimen(digitalSpecimenRecord, false, true);
          } else {
            var successfullyPublished = publishCreateEventSpecimen(digitalSpecimenRecord);
            if (!successfullyPublished) {
              rollbackDigitalRecordIds.add(digitalSpecimenRecord.id());
            }
          }
        }
    );
    return new HashSet<>(digitalSpecimenRecords).stream()
        .filter(r -> !rollbackDigitalRecordIds.contains(r.id())).collect(
            Collectors.toSet());
  }

  public Set<DigitalMediaRecord> handlePartiallyFailedElasticInsertMedia(
      Set<DigitalMediaRecord> digitalMediaRecords,
      BulkResponse bulkResponse) {
    var digitalMediaRecordMap = digitalMediaRecords.stream()
        .collect(Collectors.toMap(
            DigitalMediaRecord::id,
            Function.identity()));
    var rollbackDigitalRecordIds = new HashSet<String>();
    bulkResponse.items().forEach(
        item -> {
          var digitalMediaRecord = digitalMediaRecordMap.get(item.id());
          if (item.error() != null) {
            log.error("Failed to insert item into elastic search: {} with errors {}",
                item.id(), item.error().reason());
            rollbackDigitalRecordIds.add(item.id());
            rollbackNewMedia(digitalMediaRecord, false, true);
          } else {
            var successfullyPublished = publishCreateEventMedia(digitalMediaRecord);
            if (!successfullyPublished) {
              rollbackDigitalRecordIds.add(digitalMediaRecord.id());
            }
          }
        }
    );
    return digitalMediaRecords.stream()
        .filter(digitalMediaRecord -> !rollbackDigitalRecordIds.contains(digitalMediaRecord.id()))
        .collect(Collectors.toSet());
  }

  public Set<UpdatedDigitalSpecimenRecord> handlePartiallyFailedElasticUpdateSpecimen(
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
            var successfullyPublished = publishUpdateEventSpecimen(digitalSpecimenRecord);
            if (!successfullyPublished) {
              handleUpdatesToRollback.add(digitalSpecimenRecord);
              mutableDigitalSpecimenRecords.remove(digitalSpecimenRecord);
            }
          }
        }
    );
    filterUpdatesAndRollbackHandlesSpecimen(handleUpdatesToRollback);
    return mutableDigitalSpecimenRecords;
  }

  public Set<UpdatedDigitalMediaRecord> handlePartiallyFailedElasticUpdateMedia(
      Set<UpdatedDigitalMediaRecord> digitalMediaRecords,
      BulkResponse bulkResponse) {
    var digitalMediaMap = digitalMediaRecords.stream()
        .collect(Collectors.toMap(
            updatedDigitalMediaRecord -> updatedDigitalMediaRecord.digitalMediaRecord()
                .id(), Function.identity()));
    var digitalMediaRecordsMutable = new HashSet<>(digitalMediaRecords);
    List<UpdatedDigitalMediaRecord> handlesToRollback = new ArrayList<>();
    bulkResponse.items().forEach(
        item -> {
          var digitalMediaRecord = digitalMediaMap.get(item.id());
          if (item.error() != null) {
            log.error("Failed item to insert into elastic search: {} with errors {}",
                digitalMediaRecord.digitalMediaRecord().id(), item.error().reason());
            rollbackUpdatedMedia(digitalMediaRecord, false, true);
            handlesToRollback.add(digitalMediaRecord);
            digitalMediaRecordsMutable.remove(digitalMediaRecord);
          } else {
            var successfullyPublished = publishUpdateEventMedia(digitalMediaRecord);
            if (!successfullyPublished) {
              handlesToRollback.add(digitalMediaRecord);
              digitalMediaRecordsMutable.remove(digitalMediaRecord);
            }
          }
        }
    );
    filterUpdatesAndRollbackHandlesMedia(handlesToRollback);
    return digitalMediaRecordsMutable;
  }


  // Event publishing
  private boolean publishUpdateEventSpecimen(
      UpdatedDigitalSpecimenRecord updatedDigitalSpecimenRecord) {
    try {
      publisherService.publishUpdateEventSpecimen(
          updatedDigitalSpecimenRecord.digitalSpecimenRecord(),
          updatedDigitalSpecimenRecord.jsonPatch());
      return true;
    } catch (JsonProcessingException e) {
      log.error("Failed to publish update event", e);
      rollbackUpdatedSpecimen(updatedDigitalSpecimenRecord, true, true);
      return false;
    }
  }

  private boolean publishUpdateEventMedia(UpdatedDigitalMediaRecord updatedDigitalMediaRecord) {
    try {
      publisherService.publishUpdateEventMedia(
          updatedDigitalMediaRecord.digitalMediaRecord(),
          updatedDigitalMediaRecord.jsonPatch());
      return true;
    } catch (JsonProcessingException e) {
      log.error("Failed to publish update event", e);
      rollbackUpdatedMedia(updatedDigitalMediaRecord, true, true);
      return false;
    }
  }

  private boolean publishCreateEventSpecimen(DigitalSpecimenRecord digitalSpecimenRecord) {
    try {
      publisherService.publishCreateEventSpecimen(digitalSpecimenRecord);
    } catch (JsonProcessingException e) {
      log.error("Rolling back, failed to publish Create event", e);
      rollbackNewSpecimen(digitalSpecimenRecord, true, true);
      return false;
    }
    return true;
  }

  private boolean publishCreateEventMedia(DigitalMediaRecord digitalMediaRecord) {
    try {
      publisherService.publishCreateEventMedia(digitalMediaRecord);
    } catch (JsonProcessingException e) {
      log.error("Rolling back, failed to publish Create event", e);
      rollbackNewMedia(digitalMediaRecord, true, true);
      return false;
    }
    return true;
  }

  // Only rollback the handle updates for records that were updated
  private void filterUpdatesAndRollbackHandlesSpecimen
  (Collection<UpdatedDigitalSpecimenRecord> records) {
    var recordsToRollback = records.stream()
        .filter(
            r -> fdoRecordService.handleNeedsUpdateSpecimen(
                r.currentDigitalSpecimen().digitalSpecimenWrapper(),
                r.digitalSpecimenRecord().digitalSpecimenWrapper()))
        .map(UpdatedDigitalSpecimenRecord::currentDigitalSpecimen)
        .toList();
    var fdoRequest = fdoRecordService.buildRollbackUpdateRequest(recordsToRollback);
    rollbackHandleUpdate(recordsToRollback.stream().map(DigitalSpecimenRecord::id).toList(),
        fdoRequest);
  }

  private void filterUpdatesAndRollbackHandlesMedia
      (Collection<UpdatedDigitalMediaRecord> records) {
    var recordsToRollback = records.stream()
        .filter(
            r -> fdoRecordService.handleNeedsUpdateMedia(
                r.currentDigitalMediaRecord().attributes(),
                r.digitalMediaRecord().attributes()))
        .map(UpdatedDigitalMediaRecord::digitalMediaRecord)
        .toList();
    var fdoRequest = fdoRecordService.buildRollbackUpdateRequestMedia(recordsToRollback);
    rollbackHandleUpdate(recordsToRollback.stream().map(DigitalMediaRecord::id).toList(),
        fdoRequest);
  }

  private void rollbackHandleUpdate(List<String> ids, List<JsonNode> fdoRequest) {
    if (ids.isEmpty()) {
      return;
    }
    try {
      handleComponent.rollbackHandleUpdate(fdoRequest);
    } catch (PidException e) {
      log.error(
          "Unable to rollback handles for Updated specimens. Bad handles: {}. Revert handles to the following records: {}",
          ids);
    }
  }

}
