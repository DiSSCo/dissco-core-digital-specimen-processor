package eu.dissco.core.digitalspecimenprocessor.service;

import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.nimbusds.jose.util.Pair;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaEvent;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaRecord;
import eu.dissco.core.digitalspecimenprocessor.domain.media.UpdatedDigitalMediaRecord;
import eu.dissco.core.digitalspecimenprocessor.domain.relation.PidProcessResult;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenEvent;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenRecord;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenWrapper;
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
  private final DigitalSpecimenRepository specimenRepository;
  private final DigitalMediaRepository mediaRepository;
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
          new DigitalSpecimenEvent(updatedDigitalSpecimenRecord.enrichment(),
              updatedDigitalSpecimenRecord.digitalSpecimenRecord()
                  .digitalSpecimenWrapper(),
              updatedDigitalSpecimenRecord.digitalMediaObjectEvents()));
    } catch (JsonProcessingException e) {
      log.error(DLQ_FAILED, updatedDigitalSpecimenRecord.digitalSpecimenRecord().id(), e);
    }
  }

  private void rollBackToEarlierDatabaseVersionSpecimen(
      DigitalSpecimenRecord currentDigitalSpecimen) {
    try {
      specimenRepository.createDigitalSpecimenRecord(List.of(currentDigitalSpecimen));
    } catch (DataAccessException e) {
      log.error("Unable to rollback to previous version");
    }
  }

  private void rollBackToEarlierDatabaseVersionMedia(DigitalMediaRecord currentDigitalMedia) {
    try {
      mediaRepository.createDigitalMediaRecord(List.of(currentDigitalMedia));
    } catch (DataAccessException e) {
      log.error("Unable to rollback to previous version");
    }
  }

  // Rollback Updated Media

  public void rollbackUpdatedMedias(
      Collection<UpdatedDigitalMediaRecord> updatedDigitalMediaRecords,
      boolean elasticRollback, boolean databseRollback, List<DigitalMediaEvent> events,
      Map<String, PidProcessResult> pidMap) {
    // Get associated events for dlq purposes
    var eventMap = createRollbackMapMedia(events, pidMap);
    // Rollback in database and/or in elastic
    updatedDigitalMediaRecords.forEach(
        updatedRecord -> rollbackUpdatedMedia(updatedRecord, elasticRollback, databseRollback,
            eventMap.get(updatedRecord.digitalMediaRecord().id())));
    // Rollback handle records for those that need it
    filterUpdatesAndRollbackHandlesMedia(updatedDigitalMediaRecords);
  }

  private void rollbackUpdatedMedia(UpdatedDigitalMediaRecord updatedDigitalMediaRecord,
      boolean elasticRollback, boolean databaseRollback, DigitalMediaEvent digitalMediaEvent) {
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
      publisherService.deadLetterEventMedia(digitalMediaEvent);
    } catch (JsonProcessingException e) {
      log.error(DLQ_FAILED, updatedDigitalMediaRecord.digitalMediaRecord().id(), e);
    }
  }


  // Rollback New Specimen
  public void rollbackNewSpecimens(List<DigitalSpecimenEvent> events,
      Map<String, PidProcessResult> pidMap,
      boolean elasticRollback, boolean databaseRollback) {
    var rollbackMap = createRollbackMapSpecimen(events, pidMap);
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

  public void rollbackNewMediaSubset(List<DigitalMediaRecord> digitalMediaRecords,
      List<DigitalMediaEvent> events, Map<String, PidProcessResult> pidMap,
      boolean elasticRollback, boolean databaseRollback) {
    var rollbackAccessUris = digitalMediaRecords.stream().map(
        DigitalMediaRecord::accessURI).toList();
    var rollbackEvents = events.stream().filter(
        event -> rollbackAccessUris.contains(event.digitalMediaWrapper().accessUri())
    ).toList();
    rollbackNewMedias(rollbackEvents, pidMap, elasticRollback, databaseRollback);
  }

  private static Map<String, DigitalSpecimenEvent> createRollbackMapSpecimen(
      List<DigitalSpecimenEvent> events, Map<String, PidProcessResult> pidMap) {
    return events.stream().collect(Collectors.toMap(
        event -> pidMap.get(event.digitalSpecimenWrapper().physicalSpecimenID()).doi(),
        event -> event));
  }

  private static Map<String, DigitalMediaEvent> createRollbackMapMedia(
      List<DigitalMediaEvent> events, Map<String, PidProcessResult> pidMap) {
    return events.stream().collect(Collectors.toMap(
        event -> pidMap.get(event.digitalMediaWrapper().attributes().getAcAccessURI())
            .doi(),
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
      specimenRepository.rollbackSpecimen(id);
    }
    try {
      publisherService.deadLetterEventSpecimen(event);
    } catch (JsonProcessingException e) {
      log.error(DLQ_FAILED, id, e);
    }
  }

  // Rollback New Media
  public void rollbackNewMedias(List<DigitalMediaEvent> events,
      Map<String, PidProcessResult> pidMap,
      boolean elasticRollback, boolean databaseRollback) {
    var rollbackMap = createRollbackMapMedia(events, pidMap);
    // Rollback in database and/or elastic
    rollbackMap.forEach(
        (key, value) -> rollbackNewMedia(key, value, elasticRollback, databaseRollback));
    // Rollback handle creation for specimen
    rollbackHandleCreation(rollbackMap.keySet());
  }

  private void rollbackNewMedia(String id, DigitalMediaEvent event,
      boolean elasticRollback, boolean databaseRollback) {
    if (elasticRollback) {
      try {
        elasticRepository.rollbackSpecimen(id);
      } catch (IOException | ElasticsearchException e) {
        log.error("Fatal exception, unable to roll back: {}", id, e);
      }
    }
    if (databaseRollback) {
      specimenRepository.rollbackSpecimen(id);
    }
    try {
      publisherService.deadLetterEventMedia(event);
    } catch (JsonProcessingException e) {
      log.error(DLQ_FAILED, id, e);
    }
  }


  // Elastic Failures
  public Set<DigitalSpecimenRecord> handlePartiallyFailedElasticInsertSpecimen(
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
            var successfullyPublished = publishEventsSpecimen(digitalSpecimenRecord, event);
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

  public Set<DigitalMediaRecord> handlePartiallyFailedElasticInsertMedia(
      Set<DigitalMediaRecord> digitalMediaRecords,
      BulkResponse bulkResponse, List<DigitalMediaEvent> events) {
    var digitalMediaMap = digitalMediaRecords.stream()
        .collect(Collectors.toMap(
            DigitalMediaRecord::id,
            digitalMediaRecord -> Pair.of(digitalMediaRecord, getDigitalMediaEvent(events,
                digitalMediaRecord.accessURI())
            )));
    var rollbackDigitalRecordIds = new ArrayList<String>();
    bulkResponse.items().forEach(
        item -> {
          var digitalMediaRecord = digitalMediaMap.get(item.id()).getLeft();
          var event = digitalMediaMap.get(item.id()).getRight();
          if (item.error() != null) {
            log.error("Failed to insert item into elastic search: {} with errors {}",
                item.id(), item.error().reason());
            rollbackDigitalRecordIds.add(item.id());
            rollbackNewMedia(item.id(), event, false, true);
          } else {
            var successfullyPublished = publishEventsMedia(digitalMediaRecord, event);
            if (!successfullyPublished) {
              rollbackDigitalRecordIds.add(digitalMediaRecord.id());
            }
          }
        }
    );
    rollbackHandleCreation(rollbackDigitalRecordIds);
    return new HashSet<>(digitalMediaRecords).stream()
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

  private static DigitalMediaEvent getDigitalMediaEvent(
      List<DigitalMediaEvent> digitalMediaEvents, String accessUri) {
    for (var digitalMediaEvent : digitalMediaEvents) {
      if (accessUri.equalsIgnoreCase(
          digitalMediaEvent.digitalMediaWrapper().attributes().getAcAccessURI())) {
        return digitalMediaEvent;
      }
    }
    throw new IllegalStateException();
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
            var successfullyPublished = publishUpdateEvent(digitalSpecimenRecord);
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

  public void handlePartiallyFailedElasticUpdateMedia(
      Set<UpdatedDigitalMediaRecord> digitalMediaRecords,
      BulkResponse bulkResponse, List<DigitalMediaEvent> digitalMediaEvents,
      Map<String, PidProcessResult> pidMap) {
    var eventMap = createRollbackMapMedia(digitalMediaEvents, pidMap);
    var digitalMediaMap = digitalMediaRecords.stream()
        .collect(Collectors.toMap(
            updatedDigitalMediaRecord -> updatedDigitalMediaRecord.digitalMediaRecord()
                .id(), Function.identity()));
    List<UpdatedDigitalMediaRecord> handlesToRollback = new ArrayList<>();
    bulkResponse.items().forEach(
        item -> {
          var digitalMediaRecord = digitalMediaMap.get(item.id());
          var originalEvent = eventMap.get(item.id());
          if (item.error() != null) {
            log.error("Failed item to insert into elastic search: {} with errors {}",
                digitalMediaRecord.digitalMediaRecord().id(), item.error().reason());
            rollbackUpdatedMedia(digitalMediaRecord, false, true, originalEvent);
            handlesToRollback.add(digitalMediaRecord);
            digitalMediaRecords.remove(digitalMediaRecord);
          } else {
            var successfullyPublished = publishUpdateEventMedia(digitalMediaRecord, originalEvent);
            if (!successfullyPublished) {
              handlesToRollback.add(digitalMediaRecord);
              digitalMediaRecords.remove(digitalMediaRecord);
            }
          }
        }
    );
    filterUpdatesAndRollbackHandlesMedia(handlesToRollback);
  }


  // Event publishing
  private boolean publishUpdateEvent(UpdatedDigitalSpecimenRecord updatedDigitalSpecimenRecord) {
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

  private boolean publishUpdateEventMedia(UpdatedDigitalMediaRecord updatedDigitalMediaRecord,
      DigitalMediaEvent event) {
    try {
      publisherService.publishUpdateEventMedia(
          updatedDigitalMediaRecord.digitalMediaRecord(),
          updatedDigitalMediaRecord.jsonPatch());
      return true;
    } catch (JsonProcessingException e) {
      log.error("Failed to publish update event", e);
      rollbackUpdatedMedia(updatedDigitalMediaRecord, true, true, event);
      return false;
    }
  }

  private boolean publishEventsSpecimen(DigitalSpecimenRecord digitalSpecimenRecord,
      DigitalSpecimenEvent event) {
    try {
      publisherService.publishCreateEventSpecimen(digitalSpecimenRecord);
    } catch (JsonProcessingException e) {
      log.error("Rolling back, failed to publish Create event", e);
      rollbackNewSpecimen(digitalSpecimenRecord.id(), event, true, true);
      return false;
    }
    return true;
  }

  private boolean publishEventsMedia(DigitalMediaRecord digitalMediaRecord,
      DigitalMediaEvent event) {
    try {
      publisherService.publishCreateEventMedia(digitalMediaRecord);
    } catch (JsonProcessingException e) {
      log.error("Rolling back, failed to publish Create event", e);
      rollbackNewMedia(digitalMediaRecord.id(), event, true, true);
      return false;
    }
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
        publisherService.deadLetterEventSpecimen(event);
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
