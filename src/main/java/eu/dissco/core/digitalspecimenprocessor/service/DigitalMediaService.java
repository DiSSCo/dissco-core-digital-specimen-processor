package eu.dissco.core.digitalspecimenprocessor.service;

import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaEventWithoutDOI;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaKey;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaRecord;
import eu.dissco.core.digitalspecimenprocessor.domain.media.UpdatedDigitalMediaRecord;
import eu.dissco.core.digitalspecimenprocessor.domain.media.UpdatedDigitalMediaTuple;
import eu.dissco.core.digitalspecimenprocessor.exception.PidException;
import eu.dissco.core.digitalspecimenprocessor.repository.DigitalMediaRepository;
import eu.dissco.core.digitalspecimenprocessor.repository.ElasticSearchRepository;
import eu.dissco.core.digitalspecimenprocessor.web.HandleComponent;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
  private final KafkaPublisherService kafkaService;
  private final ObjectMapper mapper;
  private final ElasticSearchRepository elasticRepository;
  private final AnnotationPublisherService annotationPublisherService;

  public void processEqualDigitalMedia(List<DigitalMediaRecord> currentDigitalMedia) {
    var currentIds = currentDigitalMedia.stream().map(DigitalMediaRecord::id).toList();
    repository.updateLastChecked(currentIds);
    log.info("Successfully updated lastChecked for {} existing digital media",
        currentIds.size());
  }

  public Set<DigitalMediaRecord> persistNewDigitalMedia(
      List<DigitalMediaEventWithoutDOI> newRecords) {
    var newRecordList = newRecords.stream().map(DigitalMediaEventWithoutDOI::digitalMediaObjectWithoutDoi)
        .toList();
    Map<DigitalMediaKey, String> pidMap;
    var digitalMediaRecords = newRecords.stream()
        .collect(toMap(event -> mapToDigitalMediaRecord(event, pidMap),
            DigitalMediaEventWithoutDOI::enrichmentList));
    digitalMediaRecords.remove(null);
    if (digitalMediaRecords.isEmpty()) {
      return Collections.emptySet();
    }
    try {
      repository.createDigitalMediaRecord(digitalMediaRecords.keySet());
    } catch (DataAccessException e) {
      log.error("Database exception, unable to post new digital media to database", e);
      rollbackHandleCreation(new ArrayList<>(digitalMediaRecords.keySet()));
      for (var event : newRecords) {
        try {
          kafkaService.deadLetterEventMedia(event);
        } catch (JsonProcessingException e2) {
          log.error("Fatal Exception: unable to post event to DLQ", e);
        }
      }
      return Collections.emptySet();
    }
    log.info("{} digital media has been successfully committed to database",
        newRecords.size());
    try {
      var bulkResponse = elasticRepository.indexDigitalMedia(digitalMediaRecords.keySet());
      if (!bulkResponse.errors()) {
        handleSuccessfulElasticInsert(digitalMediaRecords);
      } else {
        handlePartiallyFailedElasticInsert(digitalMediaRecords, bulkResponse);
      }
      log.info("Successfully created {} new digital media", digitalMediaRecords.size());
      annotationPublisherService.publishAnnotationNewMedia(digitalMediaRecords.keySet());
      return digitalMediaRecords.keySet();
    } catch (IOException | ElasticsearchException e) {
      log.error("Rolling back, failed to insert records in elastic", e);
      digitalMediaRecords.forEach(this::rollbackNewDigitalMedia);
      var mediaRecords = digitalMediaRecords.keySet().stream().toList();
      rollbackHandleCreation(mediaRecords);
      return Collections.emptySet();
    }
  }

  private DigitalMediaRecord mapToDigitalMediaRecord(DigitalMediaEvent event,
      Map<DigitalMediaKey, String> pidMap) {
    String handle;
    if (event.digitalMediaWrapper().attributes().getId() != null) {
      handle = event.digitalMediaWrapper().attributes().getId();
    } else {
      var targetKey = new DigitalMediaKey(event.digitalMediaWrapper().digitalSpecimenID(),
          event.digitalMediaWrapper().attributes().getAcAccessURI());
      handle = pidMap.get(targetKey);
      if (handle == null) {
        log.error("Failed to process record with ds id: {} and mediaUrl: {}",
            event.digitalMediaWrapper().digitalSpecimenID(),
            event.digitalMediaWrapper().attributes().getAcAccessURI());
        return null;
      }
    }
    event.digitalMediaWrapper().attributes().setId(null);
    event.digitalMediaWrapper().attributes().setDctermsIdentifier(null);
    return new DigitalMediaRecord(
        handle,
        1,
        Instant.now(),
        event.digitalMediaWrapper()
    );
  }

  private void handleSuccessfulElasticUpdate(Set<UpdatedDigitalMediaRecord> digitalMediaRecords) {
    log.debug("Successfully indexed {} digital media", digitalMediaRecords);
    var failedRecords = new HashSet<UpdatedDigitalMediaRecord>();
    for (var digitalMediaRecord : digitalMediaRecords) {
      var successfullyPublished = publishUpdateEvent(digitalMediaRecord);
      if (!successfullyPublished) {
        failedRecords.add(digitalMediaRecord);
      }
    }
    if (!failedRecords.isEmpty()) {
      filterUpdatesAndRollbackHandles(new ArrayList<>(failedRecords));
      digitalMediaRecords.removeAll(failedRecords);
    }
  }

  private boolean publishUpdateEvent(UpdatedDigitalMediaRecord digitalMediaRecord) {
    try {
      kafkaService.publishUpdateEvent(digitalMediaRecord.digitalMediaRecord(),
          digitalMediaRecord.jsonPatch());
      return true;
    } catch (JsonProcessingException e) {
      log.error("Rolling back, failed to publish update event", e);
      rollbackUpdatedDigitalMedia(digitalMediaRecord, true);
      return false;
    }
  }

  private void handleSuccessfulElasticInsert(
      Map<DigitalMediaRecord, List<String>> digitalMediaRecords) {
    log.debug("Successfully indexed {} digital media", digitalMediaRecords);
    var recordsToRollback = new ArrayList<DigitalMediaRecord>();
    for (var entry : digitalMediaRecords.entrySet()) {
      var successfullyPublished = publishEvents(entry.getKey(), entry.getValue());
      if (!successfullyPublished) {
        recordsToRollback.add(entry.getKey());
        digitalMediaRecords.remove(entry.getKey());
      }
    }
    if (!recordsToRollback.isEmpty()) {
      rollbackHandleCreation(recordsToRollback);
    }
  }

  private boolean publishEvents(DigitalMediaRecord key, List<String> value) {
    try {
      kafkaService.publishCreateEvent(key);
    } catch (JsonProcessingException e) {
      log.error("Rolling back, failed to publish Create event", e);
      rollbackNewDigitalMedia(key, value, true);
      return false;
    }
    value.forEach(mas -> {
      try {
        kafkaService.publishAnnotationRequestEvent(mas, key);
      } catch (JsonProcessingException e) {
        log.error(
            "No action taken, failed to publish annotation request event for aas: {} digital media: {}",
            mas, key.id(), e);
      }
    });
    return true;
  }

  public Set<DigitalMediaRecord> updateExistingDigitalMedia(
      List<UpdatedDigitalMediaTuple> updatedDigitalSpecimenTuples) {
    var digitalMediaRecords = getDigitalMediaRecordMap(updatedDigitalSpecimenTuples);
    try {
      updateHandles(digitalMediaRecords);
    } catch (PidException e) {
      log.error("unable to update handle records for given request", e);
      dlqBatchUpdate(digitalMediaRecords);
      return Set.of();
    }
    log.info("Persisting to db");
    try {
      repository.createDigitalMediaRecord(
          digitalMediaRecords.stream().map(UpdatedDigitalMediaRecord::digitalMediaRecord)
              .toList());
    } catch (DataAccessException e) {
      log.error("Database exception: unable to post updates to db", e);
      rollbackHandleUpdate(new ArrayList<>(digitalMediaRecords));
      for (var updatedRecord : digitalMediaRecords) {
        try {
          kafkaService.deadLetterEvent(mapUpdatedRecordToEvent(updatedRecord));
        } catch (JsonProcessingException e2) {
          log.error("Fatal exception: unable to DLQ failed event", e);
        }
      }
      return Collections.emptySet();
    }
    log.info("Persisting to elastic");
    try {
      var bulkResponse = elasticRepository.indexDigitalMedia(
          digitalMediaRecords.stream().map(UpdatedDigitalMediaRecord::digitalMediaRecord)
              .toList());
      if (!bulkResponse.errors()) {
        handleSuccessfulElasticUpdate(digitalMediaRecords);
      } else {
        handlePartiallyElasticUpdate(digitalMediaRecords, bulkResponse);
      }
      var successfullyProcessedRecords = digitalMediaRecords.stream()
          .map(UpdatedDigitalMediaRecord::digitalMediaRecord).collect(
              toSet());
      log.info("Successfully updated {} digital media object", successfullyProcessedRecords.size());
      annotationPublisherService.publishAnnotationUpdatedMedia(digitalMediaRecords);
      return successfullyProcessedRecords;
    } catch (IOException | ElasticsearchException e) {
      log.error("Rolling back, failed to insert records in elastic", e);
      digitalMediaRecords.forEach(
          updatedDigitalMediaRecord -> rollbackUpdatedDigitalMedia(
              updatedDigitalMediaRecord,
              false));
      filterUpdatesAndRollbackHandles(new ArrayList<>(digitalMediaRecords));
      return Set.of();
    }
  }


}
