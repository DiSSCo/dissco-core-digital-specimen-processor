package eu.dissco.core.digitalspecimenprocessor.service;

import co.elastic.clients.elasticsearch.core.BulkResponse;
import eu.dissco.core.digitalspecimenprocessor.domain.DigitalSpecimen;
import eu.dissco.core.digitalspecimenprocessor.domain.DigitalSpecimenEvent;
import eu.dissco.core.digitalspecimenprocessor.domain.DigitalSpecimenRecord;
import eu.dissco.core.digitalspecimenprocessor.domain.ProcessResult;
import eu.dissco.core.digitalspecimenprocessor.domain.UpdatedDigitalSpecimenTuple;
import eu.dissco.core.digitalspecimenprocessor.repository.DigitalSpecimenRepository;
import eu.dissco.core.digitalspecimenprocessor.repository.ElasticSearchRepository;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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
  private final HandleService handleService;
  private final ElasticSearchRepository elasticRepository;
  private final KafkaPublisherService kafkaService;

  public List<DigitalSpecimenRecord> handleMessages(List<DigitalSpecimenEvent> events) {
    log.info("Processing {} digital specimen", events.size());
    var processResult = processSpecimens(events);
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

  private ProcessResult processSpecimens(List<DigitalSpecimenEvent> events) {
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
              new UpdatedDigitalSpecimenTuple(currentDigitalSpecimen, digitalSpecimen));
        }
      }
    }
    return new ProcessResult(equalSpecimens, changedSpecimens, newSpecimens);
  }

  private Map<String, DigitalSpecimenRecord> getCurrentSpecimen(List<DigitalSpecimenEvent> events) {
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
    var handleUpdates = updatedDigitalSpecimenTuples.stream().filter(
        tuple -> handleNeedsUpdate(tuple.currentSpecimen().digitalSpecimen(),
            tuple.digitalSpecimen())).toList();
    handleService.updateHandles(handleUpdates);

    var digitalSpecimenRecords = updatedDigitalSpecimenTuples.stream().collect(Collectors.toMap(
        tuple -> new DigitalSpecimenRecord(
            tuple.currentSpecimen().id(),
            calculateMidsLevel(tuple.digitalSpecimen()),
            tuple.currentSpecimen().version() + 1,
            Instant.now(),
            tuple.digitalSpecimen()
        ), UpdatedDigitalSpecimenTuple::currentSpecimen));
    log.info("Persisting to db");
    repository.createDigitalSpecimenRecord(digitalSpecimenRecords.keySet());
    log.info("Persisting to elastic");
    BulkResponse bulkResponse = null;
    try {
      bulkResponse = elasticRepository.indexDigitalSpecimen(digitalSpecimenRecords.keySet());
      if (!bulkResponse.errors()) {
        log.debug("Successfully indexed {} specimens", digitalSpecimenRecords);
        digitalSpecimenRecords.forEach(kafkaService::publishUpdateEvent);
      } else {
        var digitalSpecimenMap = digitalSpecimenRecords.values().stream()
            .collect(Collectors.toMap(DigitalSpecimenRecord::id, Function.identity()));
        bulkResponse.items().forEach(
            item -> {
              var digitalSpecimenRecord = digitalSpecimenMap.get(item.id());
              if (item.error() != null) {
                // TODO Rollback database (remove version) and move message to DLQ
                digitalSpecimenRecords.remove(digitalSpecimenRecord);
              } else {
                kafkaService.publishUpdateEvent(digitalSpecimenRecord,
                    digitalSpecimenRecords.get(digitalSpecimenRecord));
              }
            }
        );
      }
      log.info("Successfully updated {} digitalSpecimen", updatedDigitalSpecimenTuples.size());
      return digitalSpecimenRecords.keySet();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private boolean handleNeedsUpdate(DigitalSpecimen currentDigitalSpecimen,
      DigitalSpecimen digitalSpecimen) {
    return !currentDigitalSpecimen.type().equals(digitalSpecimen.type()) ||
        !currentDigitalSpecimen.organizationId().equals(digitalSpecimen.organizationId());
  }

  private Set<DigitalSpecimenRecord> createNewDigitalSpecimen(List<DigitalSpecimenEvent> events) {
    var digitalSpecimenRecords = events.stream().collect(Collectors.toMap(
        event -> {
          try {
            return new DigitalSpecimenRecord(
                handleService.createNewHandle(event.digitalSpecimen()),
                calculateMidsLevel(event.digitalSpecimen()),
                1,
                Instant.now(),
                event.digitalSpecimen()
            );
          } catch (TransformerException e) {
            log.error("Failed to process record with id: {}",
                event.digitalSpecimen().physicalSpecimenId(),
                e);
            return null;
          }
        },
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
        log.debug("Successfully indexed {} specimens", digitalSpecimenRecords);
        digitalSpecimenRecords.forEach((key, value) -> {
          kafkaService.publishCreateEvent(key);
          value.forEach(aas -> kafkaService.publishAnnotationRequestEvent(aas, key));
        });
      } else {
        var digitalSpecimenMap = digitalSpecimenRecords.keySet().stream()
            .collect(Collectors.toMap(DigitalSpecimenRecord::id, Function.identity()));
        bulkResponse.items().forEach(
            item -> {
              var digitalSpecimenRecord = digitalSpecimenMap.get(item.id());
              if (item.error() != null) {
                // TODO Rollback database and handle and move message to DLQ
                digitalSpecimenRecords.remove(digitalSpecimenRecord);
              } else {
                kafkaService.publishCreateEvent(digitalSpecimenRecord);
                digitalSpecimenRecords.get(digitalSpecimenRecord).forEach(
                    aas -> kafkaService.publishAnnotationRequestEvent(aas, digitalSpecimenRecord));
              }
            }
        );
      }
      log.info("Successfully created {} new digitalSpecimen", digitalSpecimenRecords.size());
      return digitalSpecimenRecords.keySet();
    } catch (IOException e) {
      // TODO rollback all items from database and remove handles
      throw new RuntimeException(e);
    }

  }

  private int calculateMidsLevel(DigitalSpecimen digitalSpecimen) {
    return 1;
  }
}
