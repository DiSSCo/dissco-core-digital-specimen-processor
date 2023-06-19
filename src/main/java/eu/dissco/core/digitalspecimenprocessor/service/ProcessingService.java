package eu.dissco.core.digitalspecimenprocessor.service;

import co.elastic.clients.elasticsearch.core.BulkResponse;
import com.fasterxml.jackson.core.JsonProcessingException;
import eu.dissco.core.digitalspecimenprocessor.component.FdoRecordBuilder;
import eu.dissco.core.digitalspecimenprocessor.component.HandleComponent;
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
import javax.xml.transform.TransformerException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class ProcessingService {

  private final DigitalSpecimenRepository repository;
  private final FdoRecordBuilder fdoRecordBuilder;
  private final ElasticSearchRepository elasticRepository;
  private final KafkaPublisherService kafkaService;
  private final MidsService midsService;
  private final HandleComponent handleComponent;

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
    try {
      updateHandles(updatedDigitalSpecimenTuples);
    } catch (PidCreationException | PidAuthenticationException e){
      log.info("Unable to update Handle record. Not proceeding with update");
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
    digitalSpecimenRecords.removeAll(failedRecords);
  }

  private void handlePartiallyElasticUpdate(
      Set<UpdatedDigitalSpecimenRecord> digitalSpecimenRecords,
      BulkResponse bulkResponse) {
    var digitalSpecimenMap = digitalSpecimenRecords.stream()
        .collect(Collectors.toMap(
            updatedDigitalSpecimenRecord -> updatedDigitalSpecimenRecord.digitalSpecimenRecord()
                .id(), Function.identity()));
    bulkResponse.items().forEach(
        item -> {
          var digitalSpecimenRecord = digitalSpecimenMap.get(item.id());
          if (item.error() != null) {
            log.error("Failed item to insert into elastic search: {} with errors {}",
                digitalSpecimenRecord.digitalSpecimenRecord().id(), item.error().reason());
            rollbackUpdatedSpecimen(digitalSpecimenRecord, false);
            digitalSpecimenRecords.remove(digitalSpecimenRecord);
          } else {
            var successfullyPublished = publishUpdateEvent(digitalSpecimenRecord);
            if (!successfullyPublished) {
              digitalSpecimenRecords.remove(digitalSpecimenRecord);
            }
          }
        }
    );
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

  private void updateHandles(List<UpdatedDigitalSpecimenTuple> updatedDigitalSpecimenTuples)
      throws PidCreationException, PidAuthenticationException {
    var digitalSpecimensToUpdate = updatedDigitalSpecimenTuples.stream()
        .filter(tuple -> fdoRecordBuilder.handleNeedsUpdate(
            tuple.currentSpecimen().digitalSpecimen(),
            tuple.digitalSpecimenEvent().digitalSpecimen()))
        .map(tuple -> tuple.digitalSpecimenEvent().digitalSpecimen())
        .toList();

    if (!digitalSpecimensToUpdate.isEmpty()) {
      var requests = fdoRecordBuilder.buildPostHandleRequest(digitalSpecimensToUpdate);
      handleComponent.postHandle(requests);
    }
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
    if (fdoRecordBuilder.handleNeedsUpdate(updatedDigitalSpecimenRecord.currentDigitalSpecimen().digitalSpecimen(),
        updatedDigitalSpecimenRecord.digitalSpecimenRecord()
            .digitalSpecimen())) {
      fdoRecordBuilder.deleteVersion(updatedDigitalSpecimenRecord.currentDigitalSpecimen());
    }
    try {
      kafkaService.deadLetterEvent(
          new DigitalSpecimenEvent(updatedDigitalSpecimenRecord.enrichment(),
              updatedDigitalSpecimenRecord.digitalSpecimenRecord()
                  .digitalSpecimen()));
    } catch (JsonProcessingException e) {
      log.error("Fatal exception, unable to dead letter queue: "
          + updatedDigitalSpecimenRecord.digitalSpecimenRecord().id(), e);
    }
  }

  private void rollBackToEarlierVersion(DigitalSpecimenRecord currentDigitalSpecimen) {
    repository.createDigitalSpecimenRecord(List.of(currentDigitalSpecimen));
  }


  private Set<DigitalSpecimenRecord> createNewDigitalSpecimen(List<DigitalSpecimenEvent> events) {
    var digitalSpecimenRecords = events.stream().collect(Collectors.toMap(
        this::mapToDigitalSpecimenRecord,
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
      log.info("Successfully created {} new digitalSpecimen", digitalSpecimenRecords.size());
      return digitalSpecimenRecords.keySet();
    } catch (IOException e) {
      log.error("Rolling back, failed to insert records in elastic", e);
      digitalSpecimenRecords.forEach(this::rollbackNewSpecimen);
      return Collections.emptySet();
    }
  }

  private void handleSuccessfulElasticInsert(
      Map<DigitalSpecimenRecord, List<String>> digitalSpecimenRecords) {
    log.debug("Successfully indexed {} specimens", digitalSpecimenRecords);
    for (var entry : digitalSpecimenRecords.entrySet()) {
      var successfullyPublished = publishEvents(entry.getKey(), entry.getValue());
      if (!successfullyPublished) {
        digitalSpecimenRecords.remove(entry.getKey());
      }
    }
  }

  private void handlePartiallyFailedElasticInsert(
      Map<DigitalSpecimenRecord, List<String>> digitalSpecimenRecords,
      BulkResponse bulkResponse) {
    var digitalSpecimenMap = digitalSpecimenRecords.keySet().stream()
        .collect(Collectors.toMap(DigitalSpecimenRecord::id, Function.identity()));
    bulkResponse.items().forEach(
        item -> {
          var digitalSpecimenRecord = digitalSpecimenMap.get(item.id());
          if (item.error() != null) {
            log.error("Failed item to insert into elastic search: {} with errors {}",
                digitalSpecimenRecord.id(), item.error().reason());
            rollbackNewSpecimen(digitalSpecimenRecord,
                digitalSpecimenRecords.get(digitalSpecimenRecord));
            digitalSpecimenRecords.remove(digitalSpecimenRecord);
          } else {
            var successfullyPublished = publishEvents(digitalSpecimenRecord,
                digitalSpecimenRecords.get(digitalSpecimenRecord));
            if (!successfullyPublished) {
              digitalSpecimenRecords.remove(digitalSpecimenRecord);
            }
          }
        }
    );
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
    fdoRecordBuilder.rollbackHandleCreation(digitalSpecimenRecord);
    try {
      kafkaService.deadLetterEvent(
          new DigitalSpecimenEvent(enrichments, digitalSpecimenRecord.digitalSpecimen()));
    } catch (JsonProcessingException e) {
      log.error("Fatal exception, unable to dead letter queue: " + digitalSpecimenRecord.id(), e);
    }
  }

  private DigitalSpecimenRecord mapToDigitalSpecimenRecord(DigitalSpecimenEvent event) {
    try {
      return new DigitalSpecimenRecord(
          fdoRecordBuilder.createNewHandle(event.digitalSpecimen()),
          midsService.calculateMids(event.digitalSpecimen()),
          1,
          Instant.now(),
          event.digitalSpecimen()
      );
    } catch (TransformerException | PidCreationException e) {
      log.error("Failed to process record with id: {}",
          event.digitalSpecimen().physicalSpecimenId(),
          e);
      return null;
    }
  }

}
