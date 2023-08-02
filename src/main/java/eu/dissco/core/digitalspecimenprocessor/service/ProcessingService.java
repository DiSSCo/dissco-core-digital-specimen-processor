package eu.dissco.core.digitalspecimenprocessor.service;

import co.elastic.clients.elasticsearch.core.BulkResponse;
import com.fasterxml.jackson.core.JsonProcessingException;
import eu.dissco.core.digitalspecimenprocessor.domain.DigitalSpecimen;
import eu.dissco.core.digitalspecimenprocessor.web.HandleComponent;
import eu.dissco.core.digitalspecimenprocessor.domain.DigitalSpecimenEvent;
import eu.dissco.core.digitalspecimenprocessor.domain.DigitalSpecimenRecord;
import eu.dissco.core.digitalspecimenprocessor.domain.ProcessResult;
import eu.dissco.core.digitalspecimenprocessor.domain.UpdatedDigitalSpecimenRecord;
import eu.dissco.core.digitalspecimenprocessor.domain.UpdatedDigitalSpecimenTuple;
import eu.dissco.core.digitalspecimenprocessor.exception.DisscoRepositoryException;
import eu.dissco.core.digitalspecimenprocessor.exception.PidAuthenticationException;
import eu.dissco.core.digitalspecimenprocessor.exception.PidCreationException;
import eu.dissco.core.digitalspecimenprocessor.repository.DigitalSpecimenRepository;
import eu.dissco.core.digitalspecimenprocessor.repository.ElasticSearchRepository;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class ProcessingService {

  private final DigitalSpecimenRepository repository;
  private final FdoRecordService fdoRecordService;
  private final ElasticSearchRepository elasticRepository;
  private final KafkaPublisherService kafkaService;
  private final MidsService midsService;
  private final HandleComponent handleComponent;
  private static final String DLQ_FAILED = "Fatal exception, unable to dead letter queue: ";

  public List<DigitalSpecimenRecord> handleMessages(List<DigitalSpecimenEvent> events) {
    log.info("Processing {} digital specimen", events.size());
    var uniqueBatch = removeDuplicatesInBatch(events);
    var processResult = processSpecimens(uniqueBatch);
    var results = new ArrayList<DigitalSpecimenRecord>();
    if (!processResult.equalSpecimens().isEmpty()) {
      updateEqualSpecimen(processResult.equalSpecimens());
    }
    if (!processResult.newSpecimens().isEmpty()) {
      results.addAll(createNewDigitalSpecimen(processResult.newSpecimens()));
    }
    if (!processResult.changedSpecimens().isEmpty()) {
      results.addAll(updateExistingDigitalSpecimen(processResult.changedSpecimens()));
    }
    return results;
  }

  private Set<DigitalSpecimenEvent> removeDuplicatesInBatch(List<DigitalSpecimenEvent> events) {
    var uniqueSet = new HashSet<DigitalSpecimenEvent>();
    var map = events.stream()
        .collect(Collectors.groupingBy(event -> event.digitalSpecimen().physicalSpecimenId()));
    for (Entry<String, List<DigitalSpecimenEvent>> entry : map.entrySet()) {
      if (entry.getValue().size() > 1) {
        log.warn("Found {} duplicates in batch for id {}", entry.getValue().size(), entry.getKey());
        for (int i = 0; i < entry.getValue().size(); i++) {
          if (i == 0) {
            uniqueSet.add(entry.getValue().get(i));
          } else {
            republishEvent(entry.getValue().get(i));
          }
        }
      } else {
        uniqueSet.add(entry.getValue().get(0));
      }
    }
    return uniqueSet;
  }

  private ProcessResult processSpecimens(Set<DigitalSpecimenEvent> events) {
    try {
      var currentSpecimens = getCurrentSpecimen(events);
      var equalSpecimens = new ArrayList<DigitalSpecimenRecord>();
      var changedSpecimens = new ArrayList<UpdatedDigitalSpecimenTuple>();
      var newSpecimens = new ArrayList<DigitalSpecimenEvent>();

      for (DigitalSpecimenEvent event : events) {
        var digitalSpecimen = event.digitalSpecimen();
        log.debug("ds: {}", digitalSpecimen);
        if (!currentSpecimens.containsKey(digitalSpecimen.physicalSpecimenId())) {
          log.debug("Specimen with id: {} is completely new", digitalSpecimen.physicalSpecimenId());
          newSpecimens.add(event);
        } else {
          var currentDigitalSpecimen = currentSpecimens.get(digitalSpecimen.physicalSpecimenId());
          if (currentDigitalSpecimen.digitalSpecimen().equals(digitalSpecimen)) {
            log.debug("Received digital specimen is equal to digital specimen: {}",
                currentDigitalSpecimen.id());
            equalSpecimens.add(currentDigitalSpecimen);
          } else {
            log.debug("Specimen with id: {} has received an update", currentDigitalSpecimen.id());
            changedSpecimens.add(
                new UpdatedDigitalSpecimenTuple(currentDigitalSpecimen, event));
          }
        }
      }
      return new ProcessResult(equalSpecimens, changedSpecimens, newSpecimens);
    } catch (DisscoRepositoryException ex) {
      log.error("Republishing messages, Unable to retrieve current specimen from repository", ex);
      events.forEach(this::republishEvent);
      return new ProcessResult(List.of(), List.of(), List.of());
    }
  }

  private void republishEvent(DigitalSpecimenEvent event) {
    try {
      kafkaService.republishEvent(event);
    } catch (JsonProcessingException e) {
      log.error("Fatal exception, unable to republish message due to invalid json", e);
    }
  }

  private Map<String, DigitalSpecimenRecord> getCurrentSpecimen(Set<DigitalSpecimenEvent> events)
      throws DisscoRepositoryException {
    return repository.getDigitalSpecimens(
            events.stream().map(event -> event.digitalSpecimen().physicalSpecimenId()).toList())
        .stream().collect(
            Collectors.toMap(
                specimenRecord -> specimenRecord.digitalSpecimen().physicalSpecimenId(),
                Function.identity()));
  }

  private void updateEqualSpecimen(List<DigitalSpecimenRecord> currentDigitalSpecimen) {
    var currentIds = currentDigitalSpecimen.stream().map(DigitalSpecimenRecord::id).toList();
    repository.updateLastChecked(currentIds);
    log.info("Successfully updated lastChecked for {} existing digitalSpecimen",
        currentDigitalSpecimen.size());
  }

  private Set<DigitalSpecimenRecord> updateExistingDigitalSpecimen(
      List<UpdatedDigitalSpecimenTuple> updatedDigitalSpecimenTuples) {

    log.info("Persisting to Handle Server");
    var successfullyUpdatedHandles = updateHandles(updatedDigitalSpecimenTuples);
    if (!successfullyUpdatedHandles) {
      return Set.of();
    }

    var digitalSpecimenRecords = getSpecimenRecordMap(updatedDigitalSpecimenTuples);

    log.info("Persisting to db");
    repository.createDigitalSpecimenRecord(
        digitalSpecimenRecords.stream().map(UpdatedDigitalSpecimenRecord::digitalSpecimenRecord)
            .toList());
    log.info("Persisting to elastic");
    try {
      var bulkResponse = elasticRepository.indexDigitalSpecimen(
          digitalSpecimenRecords.stream().map(UpdatedDigitalSpecimenRecord::digitalSpecimenRecord)
              .toList());
      if (!bulkResponse.errors()) {
        handleSuccessfulElasticUpdate(digitalSpecimenRecords);
      } else {
        handlePartiallyElasticUpdate(digitalSpecimenRecords, bulkResponse);
      }
      var successfullyProcessedRecords = digitalSpecimenRecords.stream()
          .map(UpdatedDigitalSpecimenRecord::digitalSpecimenRecord).collect(
              Collectors.toSet());
      log.info("Successfully updated {} digitalSpecimen", successfullyProcessedRecords.size());
      return successfullyProcessedRecords;
    } catch (IOException e) {
      log.error("Rolling back, failed to insert records in elastic", e);
      digitalSpecimenRecords.forEach(
          updatedDigitalSpecimenRecord -> rollbackUpdatedSpecimen(updatedDigitalSpecimenRecord,
              false));
      filterUpdatesAndRollbackHandles(updatedDigitalSpecimenTuples);
      return Set.of();
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
      var failedRecordsCurrent = failedRecords.stream()
          .map(UpdatedDigitalSpecimenRecord::currentDigitalSpecimen).toList();
      rollbackHandleUpdate(failedRecordsCurrent);
    }
    digitalSpecimenRecords.removeAll(failedRecords);
  }

  private void handlePartiallyElasticUpdate(
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
            rollbackUpdatedSpecimen(digitalSpecimenRecord, false);
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
  }

  private Set<UpdatedDigitalSpecimenRecord> getSpecimenRecordMap(
      List<UpdatedDigitalSpecimenTuple> updatedDigitalSpecimenTuples) {
    return updatedDigitalSpecimenTuples.stream().map(tuple -> new UpdatedDigitalSpecimenRecord(
        new DigitalSpecimenRecord(
            tuple.currentSpecimen().id(),
            midsService.calculateMids(tuple.digitalSpecimenEvent().digitalSpecimen()),
            tuple.currentSpecimen().version() + 1,
            Instant.now(),
            tuple.digitalSpecimenEvent().digitalSpecimen()),
        tuple.digitalSpecimenEvent().enrichmentList(),
        tuple.currentSpecimen()
    )).collect(Collectors.toSet());
  }

  private boolean updateHandles(List<UpdatedDigitalSpecimenTuple> updatedDigitalSpecimenTuples) {
    var digitalSpecimensToUpdate = updatedDigitalSpecimenTuples.stream()
        .filter(tuple -> fdoRecordService.handleNeedsUpdate(
            tuple.currentSpecimen().digitalSpecimen(),
            tuple.digitalSpecimenEvent().digitalSpecimen()))
        .map(tuple -> tuple.digitalSpecimenEvent().digitalSpecimen())
        .toList();

    if (!digitalSpecimensToUpdate.isEmpty()) {
      try {
        var requests = fdoRecordService.buildPostHandleRequest(digitalSpecimensToUpdate);
        handleComponent.postHandle(requests);
      } catch (PidCreationException | PidAuthenticationException e) {
        log.error("Unable to update Handle record. Not proceeding with update. ", e);
        try {
          for (var tuple : updatedDigitalSpecimenTuples) {
            kafkaService.deadLetterEvent(tuple.digitalSpecimenEvent());
          }
        } catch (JsonProcessingException jsonEx) {
          log.error(DLQ_FAILED + updatedDigitalSpecimenTuples, jsonEx);
        }
        return false;
      }
    }
    return true;
  }

  private boolean publishUpdateEvent(UpdatedDigitalSpecimenRecord updatedDigitalSpecimenRecord) {
    try {
      kafkaService.publishUpdateEvent(updatedDigitalSpecimenRecord.digitalSpecimenRecord(),
          updatedDigitalSpecimenRecord.currentDigitalSpecimen());
      return true;
    } catch (JsonProcessingException e) {
      log.error("Rolling back, failed to publish update event", e);
      rollbackUpdatedSpecimen(updatedDigitalSpecimenRecord, true);
      return false;
    }
  }

  private void rollbackUpdatedSpecimen(UpdatedDigitalSpecimenRecord updatedDigitalSpecimenRecord,
      boolean elasticRollback) {
    if (elasticRollback) {
      try {
        elasticRepository.rollbackVersion(updatedDigitalSpecimenRecord.currentDigitalSpecimen());
      } catch (IOException e) {
        log.error("Fatal exception, unable to roll back update for: "
            + updatedDigitalSpecimenRecord.currentDigitalSpecimen(), e);
      }
    }
    rollBackToEarlierVersion(updatedDigitalSpecimenRecord.currentDigitalSpecimen());
    try {
      kafkaService.deadLetterEvent(
          new DigitalSpecimenEvent(updatedDigitalSpecimenRecord.enrichment(),
              updatedDigitalSpecimenRecord.digitalSpecimenRecord()
                  .digitalSpecimen()));
    } catch (JsonProcessingException e) {
      log.error(DLQ_FAILED + updatedDigitalSpecimenRecord.digitalSpecimenRecord().id(), e);
    }
  }

  private void rollBackToEarlierVersion(DigitalSpecimenRecord currentDigitalSpecimen) {
    repository.createDigitalSpecimenRecord(List.of(currentDigitalSpecimen));
  }


  private Set<DigitalSpecimenRecord> createNewDigitalSpecimen(List<DigitalSpecimenEvent> events) {
    var startTime = Instant.now();
    int eventsNum = events.size();
    Map<String, String> pidMap;
    try {
      pidMap = createNewPidRecords(events);
    } catch (PidAuthenticationException | PidCreationException e) {
      log.error("Unable to create PID. {}", e.getMessage());
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
      return Collections.emptySet();
    }
    var digitalSpecimenRecords = events.stream().collect(Collectors.toMap(
        event -> mapToDigitalSpecimenRecord(event, pidMap),
        DigitalSpecimenEvent::enrichmentList
    ));
    digitalSpecimenRecords.remove(null);
    if (digitalSpecimenRecords.isEmpty()) {
      return Collections.emptySet();
    }
    repository.createDigitalSpecimenRecord(digitalSpecimenRecords.keySet());

    try {
      var bulkResponse = elasticRepository.indexDigitalSpecimen(digitalSpecimenRecords.keySet());
      if (!bulkResponse.errors()) {
        handleSuccessfulElasticInsert(digitalSpecimenRecords);
      } else {
        handlePartiallyFailedElasticInsert(digitalSpecimenRecords, bulkResponse);
      }
    } catch (IOException e) {
      log.error("Rolling back, failed to insert records in elastic", e);
      digitalSpecimenRecords.forEach(this::rollbackNewSpecimen);
      rollbackHandleCreation(digitalSpecimenRecords.keySet().stream().toList());
      return Collections.emptySet();
    }
    try {
      var processEnd = Instant.now();
      handleComponent.registerDois(new ArrayList<>(pidMap.values()));
      var doiEnd = Instant.now();
      logTimes(startTime, processEnd, doiEnd, eventsNum);
    } catch (PidAuthenticationException | PidCreationException e){
      log.error("Unable to create DOIs for new Specimens", e);
    }
    return digitalSpecimenRecords.keySet();
  }

  private void logTimes(Instant startTime, Instant processEnd, Instant doiEnd, int eventsNum){
    try {
      double processTime = (double) Duration.between(startTime, processEnd).toNanos() / 1000000000;
      double doiTime = (double) Duration.between(processEnd, doiEnd).toNanos() / 1000000000;
      double totalTime = (double) Duration.between(startTime, doiEnd).toNanos() /1000000000;
      log.info("***** Performance report for {} specimens ****", eventsNum);
      log.info("\tPROCESSING TIME: {} seconds ", processTime);
      log.info("\tProcess rate: {} specimens/second", eventsNum/processTime);
      log.info("\tDOI TIME: {} seconds ", doiTime);
      log.info("\tDOI Rate: {} specimen/second", eventsNum/doiTime);
      log.info("\tTOTAL Elapsed time: {} seconds", totalTime);
      log.info("\tTotal Rate: {} specimen/second", eventsNum/totalTime);
      log.info("************************************");
    } catch (Exception e){
      log.info("Not logging times during test");
    }

  }

  private Map<String, String> createNewPidRecords(List<DigitalSpecimenEvent> events)
      throws PidCreationException, PidAuthenticationException {
    var specimenList = events.stream().map(DigitalSpecimenEvent::digitalSpecimen).toList();
    var request = fdoRecordService.buildPostHandleRequest(specimenList);
    return handleComponent.postHandle(request);
  }

  private void handleSuccessfulElasticInsert(
      Map<DigitalSpecimenRecord, List<String>> digitalSpecimenRecords) {
    log.debug("Successfully indexed {} specimens", digitalSpecimenRecords);
    List<DigitalSpecimenRecord> rollbackRecords = new ArrayList<>();
    for (var entry : digitalSpecimenRecords.entrySet()) {
      var successfullyPublished = publishEvents(entry.getKey(), entry.getValue());
      if (!successfullyPublished) {
        digitalSpecimenRecords.remove(entry.getKey());
        rollbackRecords.add(entry.getKey());
      }
    }
    if (!rollbackRecords.isEmpty()) {
      rollbackHandleCreation(rollbackRecords);
    }
  }

  private void handlePartiallyFailedElasticInsert(
      Map<DigitalSpecimenRecord, List<String>> digitalSpecimenRecords,
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
                digitalSpecimenRecords.get(digitalSpecimenRecord));
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
    rollbackHandleCreation(rollbackDigitalRecords);
  }

  private boolean publishEvents(DigitalSpecimenRecord key, List<String> value) {
    try {
      kafkaService.publishCreateEvent(key);
    } catch (JsonProcessingException e) {
      log.error("Rolling back, failed to publish Create event", e);
      rollbackNewSpecimen(key, value, true);
      return false;
    }
    value.forEach(aas -> {
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

  private void rollbackNewSpecimen(DigitalSpecimenRecord digitalSpecimenRecord,
      List<String> enrichments) {
    rollbackNewSpecimen(digitalSpecimenRecord, enrichments, false);
  }

  private void rollbackNewSpecimen(DigitalSpecimenRecord digitalSpecimenRecord,
      List<String> enrichments, boolean elasticRollback) {
    if (elasticRollback) {
      try {
        elasticRepository.rollbackSpecimen(digitalSpecimenRecord);
      } catch (IOException e) {
        log.error("Fatal exception, unable to roll back: " + digitalSpecimenRecord.id(), e);
      }
    }
    repository.rollbackSpecimen(digitalSpecimenRecord.id());
    try {
      kafkaService.deadLetterEvent(
          new DigitalSpecimenEvent(enrichments, digitalSpecimenRecord.digitalSpecimen()));
    } catch (JsonProcessingException e) {
      log.error(DLQ_FAILED + digitalSpecimenRecord.id(), e);
    }
  }

  private void rollbackHandleCreation(List<DigitalSpecimenRecord> records) {
    var request = fdoRecordService.buildRollbackCreationRequest(records);
    try {
      handleComponent.rollbackHandleCreation(request);
    } catch (PidCreationException | PidAuthenticationException e) {
      var ids = records.stream().map(DigitalSpecimenRecord::id).toList();
      log.error("Unable to rollback handles for new specimens. Bad handles: {}", ids);
    }
  }

  private void filterUpdatesAndRollbackHandles(List<UpdatedDigitalSpecimenTuple> records) {
    var recordsToRollback = records.stream()
        .filter(r -> fdoRecordService.handleNeedsUpdate(r.currentSpecimen().digitalSpecimen(),
            r.digitalSpecimenEvent().digitalSpecimen()))
        .map(UpdatedDigitalSpecimenTuple::currentSpecimen)
        .toList();
    rollbackHandleUpdate(recordsToRollback);
  }

  private void rollbackHandleUpdate(List<DigitalSpecimenRecord> recordsToRollback) {
    try {
      var request = fdoRecordService.buildRollbackUpdateRequest(recordsToRollback);
      handleComponent.rollbackHandleUpdate(request);
    } catch (PidCreationException | PidAuthenticationException e) {
      var ids = recordsToRollback.stream().map(DigitalSpecimenRecord::id).toList();
      log.error(
          "Unable to rollback handles for Updated specimens. Bad handles: {}. Revert handles to the following records: {}",
          ids, recordsToRollback);
    }
  }

  private DigitalSpecimenRecord mapToDigitalSpecimenRecord(DigitalSpecimenEvent event,
      Map<String, String> pidMap) {
    var handle = pidMap.get(event.digitalSpecimen().physicalSpecimenId());
    if (handle == null) {
      try {
        log.error("handle not created for Digital Specimen {}",
            event.digitalSpecimen().physicalSpecimenId());
        kafkaService.deadLetterEvent(event);
      } catch (JsonProcessingException e) {
        log.error("Kafka DLQ failed for specimen {}", event.digitalSpecimen().physicalSpecimenId());
      }
      return null;
    }

    return new DigitalSpecimenRecord(
        pidMap.get(event.digitalSpecimen().physicalSpecimenId()),
        midsService.calculateMids(event.digitalSpecimen()),
        1,
        Instant.now(),
        event.digitalSpecimen()
    );
  }
}

