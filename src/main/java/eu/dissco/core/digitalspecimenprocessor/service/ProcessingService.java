package eu.dissco.core.digitalspecimenprocessor.service;

import static eu.dissco.core.digitalspecimenprocessor.configuration.ApplicationConfiguration.DATE_STRING;

import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.nimbusds.jose.util.Pair;
import eu.dissco.core.digitalspecimenprocessor.domain.DigitalMediaEvent;
import eu.dissco.core.digitalspecimenprocessor.domain.DigitalMediaEventWithoutDOI;
import eu.dissco.core.digitalspecimenprocessor.domain.DigitalMediaWrapper;
import eu.dissco.core.digitalspecimenprocessor.domain.DigitalSpecimenEvent;
import eu.dissco.core.digitalspecimenprocessor.domain.DigitalSpecimenRecord;
import eu.dissco.core.digitalspecimenprocessor.domain.DigitalSpecimenWrapper;
import eu.dissco.core.digitalspecimenprocessor.domain.ProcessResult;
import eu.dissco.core.digitalspecimenprocessor.domain.UpdatedDigitalSpecimenRecord;
import eu.dissco.core.digitalspecimenprocessor.domain.UpdatedDigitalSpecimenTuple;
import eu.dissco.core.digitalspecimenprocessor.exception.DisscoRepositoryException;
import eu.dissco.core.digitalspecimenprocessor.exception.PidAuthenticationException;
import eu.dissco.core.digitalspecimenprocessor.exception.PidCreationException;
import eu.dissco.core.digitalspecimenprocessor.property.ApplicationProperties;
import eu.dissco.core.digitalspecimenprocessor.repository.DigitalSpecimenRepository;
import eu.dissco.core.digitalspecimenprocessor.repository.ElasticSearchRepository;
import eu.dissco.core.digitalspecimenprocessor.schema.Agent;
import eu.dissco.core.digitalspecimenprocessor.schema.Agent.Type;
import eu.dissco.core.digitalspecimenprocessor.schema.EntityRelationship;
import eu.dissco.core.digitalspecimenprocessor.web.HandleComponent;
import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jooq.exception.DataAccessException;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class ProcessingService {

  private static final String DLQ_FAILED = "Fatal exception, unable to dead letter queue: ";
  private final DateTimeFormatter formatter = DateTimeFormatter.ofPattern(DATE_STRING)
      .withZone(ZoneOffset.UTC);
  private final DigitalSpecimenRepository repository;
  private final FdoRecordService fdoRecordService;
  private final ElasticSearchRepository elasticRepository;
  private final KafkaPublisherService kafkaService;
  private final MidsService midsService;
  private final HandleComponent handleComponent;
  private final ApplicationProperties applicationProperties;

  public List<DigitalSpecimenRecord> handleMessages(List<DigitalSpecimenEvent> events) {
    log.info("Processing {} digital specimen", events.size());
    var uniqueBatch = removeDuplicatesInBatch(events);
    var processResult = processSpecimens(uniqueBatch);
    log.info("Batch consists of: {} new, {} update and {} equal specimen",
        processResult.newSpecimens().size(), processResult.changedSpecimens().size(),
        processResult.equalSpecimens().size());
    var results = new ArrayList<DigitalSpecimenRecord>();
    if (!processResult.equalSpecimens().isEmpty()) {
      updateEqualSpecimen(processResult.equalSpecimens(), events);
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
    var uniqueSet = new LinkedHashSet<DigitalSpecimenEvent>();
    var map = events.stream()
        .collect(
            Collectors.groupingBy(event -> event.digitalSpecimenWrapper().physicalSpecimenID()));
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
        var digitalSpecimenWrapper = event.digitalSpecimenWrapper();
        log.debug("ds: {}", digitalSpecimenWrapper);
        if (!currentSpecimens.containsKey(digitalSpecimenWrapper.physicalSpecimenID())) {
          log.debug("Specimen with id: {} is completely new",
              digitalSpecimenWrapper.physicalSpecimenID());
          newSpecimens.add(event);
        } else {
          var currentDigitalSpecimen = currentSpecimens.get(
              digitalSpecimenWrapper.physicalSpecimenID());
          if (isEqual(currentDigitalSpecimen.digitalSpecimenWrapper(), digitalSpecimenWrapper)) {
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

  /*
  We need to remove the Modified and EntityRelationshipDate from the comparison.
  We cannot control the equals in the EntityRelationship as it is generated
  Therefore we first store the EntityRelationshipDate in a map and set both to null
  When the objects are not equal we reinsert the data into the EntityRelationship

  To establish equality , we only compare type and attributes, not original data or
  physical specimen id. We ignore original data because original data is not updated
  if it does change, and we ignore physical specimen id because that's how the specimens
  were identified to be the same in the first place.
  */
  private boolean isEqual(DigitalSpecimenWrapper currentDigitalSpecimen,
      DigitalSpecimenWrapper digitalSpecimen) {
    if (currentDigitalSpecimen.attributes() == null) {
      return false;
    }
    var currentModified = currentDigitalSpecimen.attributes().getDctermsModified();
    currentDigitalSpecimen.attributes().setDctermsModified(null);
    digitalSpecimen.attributes().setDctermsModified(null);
    var entityRelationships = digitalSpecimen.attributes().getOdsHasEntityRelationship();
    digitalSpecimen.attributes().setOdsHasEntityRelationship(
        entityRelationships.stream().map(this::deepcopyEntityRelationship).toList());
    ignoreTimestampEntityRelationship(
        currentDigitalSpecimen.attributes().getOdsHasEntityRelationship());
    verifyOriginalData(currentDigitalSpecimen, digitalSpecimen);
    if (currentDigitalSpecimen.type().equals(digitalSpecimen.type()) &&
        currentDigitalSpecimen.attributes().equals(digitalSpecimen.attributes())) {
      digitalSpecimen.attributes().setOdsHasEntityRelationship(entityRelationships);
      digitalSpecimen.attributes().setDctermsModified(currentModified);
      currentDigitalSpecimen.attributes().setDctermsModified(currentModified);
      return true;
    } else {
      digitalSpecimen.attributes().setOdsHasEntityRelationship(entityRelationships);
      digitalSpecimen.attributes().setDctermsModified(formatter.format(Instant.now()));
      currentDigitalSpecimen.attributes().setDctermsModified(currentModified);
      return false;
    }
  }

  private void verifyOriginalData(DigitalSpecimenWrapper currentDigitalSpecimen,
      DigitalSpecimenWrapper digitalSpecimen){
    var currentOriginalData = currentDigitalSpecimen.originalAttributes();
    var originalData = digitalSpecimen.originalAttributes();
    if (currentOriginalData != null && !currentOriginalData.equals(originalData)){
      log.info("Original data for specimen with physical id {} has changed. Ignoring new original data.", digitalSpecimen.physicalSpecimenID());
    }
  }

  private EntityRelationship deepcopyEntityRelationship(EntityRelationship entityRelationships) {
    return new EntityRelationship()
        .withId(entityRelationships.getId())
        .withType(entityRelationships.getType())
        .withDwcRelationshipEstablishedDate(null)
        .withDwcRelationshipOfResource(entityRelationships.getDwcRelationshipOfResource())
        .withDwcRelationshipOfResourceID(entityRelationships.getDwcRelationshipOfResourceID())
        .withDwcRelatedResourceID(entityRelationships.getDwcRelatedResourceID())
        .withOdsRelatedResourceURI(entityRelationships.getOdsRelatedResourceURI())
        .withDwcRelationshipAccordingTo(entityRelationships.getDwcRelationshipAccordingTo())
        .withDwcRelationshipAccordingTo(entityRelationships.getDwcRelationshipAccordingTo())
        .withOdsEntityRelationshipOrder(entityRelationships.getOdsEntityRelationshipOrder())
        .withDwcRelationshipRemarks(entityRelationships.getDwcRelationshipRemarks());
  }

  private void ignoreTimestampEntityRelationship(List<EntityRelationship> entityRelationship) {
    entityRelationship.forEach(e -> e.setDwcRelationshipEstablishedDate(null));
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
            events.stream().map(event -> event.digitalSpecimenWrapper().physicalSpecimenID()).toList())
        .stream().collect(
            Collectors.toMap(
                specimenRecord -> specimenRecord.digitalSpecimenWrapper().physicalSpecimenID(),
                Function.identity()));
  }

  private void updateEqualSpecimen(List<DigitalSpecimenRecord> currentDigitalSpecimen,
      List<DigitalSpecimenEvent> events) {
    var currentIds = currentDigitalSpecimen.stream().map(DigitalSpecimenRecord::id).toList();
    repository.updateLastChecked(currentIds);
    log.info("Successfully updated lastChecked for {} existing digitalSpecimenWrapper",
        currentDigitalSpecimen.size());
    gatherDigitalMediaObjectForEqualRecords(currentDigitalSpecimen, events);
  }

  private void gatherDigitalMediaObjectForEqualRecords(
      List<DigitalSpecimenRecord> currentDigitalSpecimen, List<DigitalSpecimenEvent> events) {
    var pidMap = currentDigitalSpecimen.stream().collect(Collectors.toMap(
        digitalSpecimenRecord -> digitalSpecimenRecord.digitalSpecimenWrapper()
            .physicalSpecimenID(),
        DigitalSpecimenRecord::id
    ));
    for (var event : events) {
      var digitalSpecimenPid = pidMap.get(event.digitalSpecimenWrapper().physicalSpecimenID());
      var digitalMedia = event.digitalMediaEvents();
      publishDigitalMediaRecord(digitalMedia, digitalSpecimenPid);
    }
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
    try {
      repository.createDigitalSpecimenRecord(
          digitalSpecimenRecords.stream().map(UpdatedDigitalSpecimenRecord::digitalSpecimenRecord)
              .toList());
    } catch (DataAccessException e) {
      log.error("Unable to update records into database. Rolling back updates", e);
      unableToInsertUpdates(digitalSpecimenRecords);
      return Collections.emptySet();
    }
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
      log.info("Successfully updated {} digitalSpecimenWrapper",
          successfullyProcessedRecords.size());
      gatherDigitalMediaObjectForUpdatedRecords(digitalSpecimenRecords);
      return successfullyProcessedRecords;
    } catch (IOException | ElasticsearchException e) {
      log.error("Rolling back, failed to insert records in elastic", e);
      digitalSpecimenRecords.forEach(
          updatedDigitalSpecimenRecord -> rollbackUpdatedSpecimen(updatedDigitalSpecimenRecord,
              false));
      filterUpdatesAndRollbackHandles(updatedDigitalSpecimenTuples);
      return Set.of();
    }
  }

  private void unableToInsertUpdates(
      Set<UpdatedDigitalSpecimenRecord> updatedDigitalSpecimenTuples) {
    rollbackHandleUpdate(updatedDigitalSpecimenTuples.stream().map(
        UpdatedDigitalSpecimenRecord::digitalSpecimenRecord).toList());
    for (var dsRecord : updatedDigitalSpecimenTuples) {
      var event = new DigitalSpecimenEvent(
          null,
          dsRecord.digitalSpecimenRecord().digitalSpecimenWrapper(),
          dsRecord.digitalMediaObjectEvents()
      );
      try {
        kafkaService.deadLetterEvent(event);
      } catch (JsonProcessingException e) {
        log.error("Fatal Exception: Unable to DLQ failed specimens", e);
      }
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
            midsService.calculateMids(tuple.digitalSpecimenEvent().digitalSpecimenWrapper()),
            tuple.currentSpecimen().version() + 1,
            Instant.now(),
            tuple.digitalSpecimenEvent().digitalSpecimenWrapper()),
        tuple.digitalSpecimenEvent().enrichmentList(),
        tuple.currentSpecimen(),
        tuple.digitalSpecimenEvent().digitalMediaEvents()
    )).collect(Collectors.toSet());
  }

  private boolean updateHandles(List<UpdatedDigitalSpecimenTuple> updatedDigitalSpecimenTuples) {
    var digitalSpecimensToUpdate = updatedDigitalSpecimenTuples.stream()
        .filter(tuple -> fdoRecordService.handleNeedsUpdate(
            tuple.currentSpecimen().digitalSpecimenWrapper(),
            tuple.digitalSpecimenEvent().digitalSpecimenWrapper()))
        .toList();

    if (!digitalSpecimensToUpdate.isEmpty()) {
      try {
        var requests = fdoRecordService.buildUpdateHandleRequest(digitalSpecimensToUpdate);
        handleComponent.updateHandle(requests);
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
      } catch (IOException | ElasticsearchException e) {
        log.error("Fatal exception, unable to roll back update for: "
            + updatedDigitalSpecimenRecord.currentDigitalSpecimen(), e);
      }
    }
    rollBackToEarlierVersion(updatedDigitalSpecimenRecord.currentDigitalSpecimen());
    try {
      kafkaService.deadLetterEvent(
          new DigitalSpecimenEvent(updatedDigitalSpecimenRecord.enrichment(),
              updatedDigitalSpecimenRecord.digitalSpecimenRecord()
                  .digitalSpecimenWrapper(),
              updatedDigitalSpecimenRecord.digitalMediaObjectEvents()));
    } catch (JsonProcessingException e) {
      log.error(DLQ_FAILED + updatedDigitalSpecimenRecord.digitalSpecimenRecord().id(), e);
    }
  }

  private void rollBackToEarlierVersion(DigitalSpecimenRecord currentDigitalSpecimen) {
    try {
      repository.createDigitalSpecimenRecord(List.of(currentDigitalSpecimen));
    } catch (DataAccessException e) {
      log.error("Unable to rollback to previous version");
    }
  }


  private Set<DigitalSpecimenRecord> createNewDigitalSpecimen(List<DigitalSpecimenEvent> events) {
    Map<String, String> pidMap;
    try {
      pidMap = createNewPidRecords(events);
    } catch (PidAuthenticationException | PidCreationException e) {
      log.error("Unable to create PID. {}", e.getMessage());
      pidCreationFailed(events);
      return Collections.emptySet();
    }
    var digitalSpecimenRecords = events.stream().collect(Collectors.toMap(
        event -> mapToDigitalSpecimenRecord(event, pidMap),
        event -> Pair.of(event.enrichmentList(), event.digitalMediaEvents())
    ));
    digitalSpecimenRecords.remove(null);
    if (digitalSpecimenRecords.isEmpty()) {
      return Collections.emptySet();
    }
    log.info("Inserting {} new specimen into the database", digitalSpecimenRecords.size());
    try {
      repository.createDigitalSpecimenRecord(digitalSpecimenRecords.keySet());
    } catch (DataAccessException e) {
      log.error("Unable to insert new specimens into the database. Rolling back handles", e);
      unableToInsertNewSpecimens(digitalSpecimenRecords.keySet(), events);
      return Collections.emptySet();
    }
    try {
      log.info("Inserting {} new specimen into the elastic search", digitalSpecimenRecords.size());
      var bulkResponse = elasticRepository.indexDigitalSpecimen(digitalSpecimenRecords.keySet());
      if (!bulkResponse.errors()) {
        handleSuccessfulElasticInsert(digitalSpecimenRecords);
      } else {
        handlePartiallyFailedElasticInsert(digitalSpecimenRecords, bulkResponse);
      }
      log.info("Successfully created {} new digitalSpecimenWrapper", digitalSpecimenRecords.size());
      gatherDigitalMediaObjectForNewRecords(digitalSpecimenRecords);
      return digitalSpecimenRecords.keySet();
    } catch (IOException | ElasticsearchException e) {
      log.error("Rolling back, failed to insert records in elastic", e);
      digitalSpecimenRecords.forEach(this::rollbackNewSpecimen);
      rollbackHandleCreation(digitalSpecimenRecords.keySet().stream().toList());
      return Collections.emptySet();
    }
  }

  private void unableToInsertNewSpecimens(Set<DigitalSpecimenRecord> digitalSpecimenRecords,
      List<DigitalSpecimenEvent> events) {
    rollbackHandleCreation(new ArrayList<>(digitalSpecimenRecords));
    for (var event : events) {
      try {
        kafkaService.deadLetterEvent(event);
      } catch (JsonProcessingException e1) {
        log.error("Unable to DLQ failed specimens");
      }
    }
  }

  private void gatherDigitalMediaObjectForNewRecords(
      Map<DigitalSpecimenRecord, Pair<List<String>, List<DigitalMediaEventWithoutDOI>>> digitalSpecimenRecords) {
    log.info("Publishing digital media object events for processing");
    digitalSpecimenRecords.forEach((key, value) -> {
      var digitalSpecimenPid = key.id();
      var digitalMedia = value.getRight();
      publishDigitalMediaRecord(digitalMedia, digitalSpecimenPid);
    });
  }

  private void publishDigitalMediaRecord(List<DigitalMediaEventWithoutDOI> digitalMedia,
      String digitalSpecimenPid) {
    for (var digitalMediaObjectEventWithoutDoi : digitalMedia) {
      var attributes = digitalMediaObjectEventWithoutDoi.digitalMediaObjectWithoutDoi()
          .attributes();
      attributes.getOdsHasEntityRelationship().add(
          new EntityRelationship()
              .withType("ods:EntityRelationship")
              .withDwcRelationshipEstablishedDate(Date.from(Instant.now()))
              .withDwcRelationshipOfResource("hasDigitalSpecimen")
              .withDwcRelationshipAccordingTo(applicationProperties.getName())
              .withOdsRelationshipAccordingToAgent(new Agent()
                  .withType(Type.AS_APPLICATION)
                  .withId(applicationProperties.getPid())
                  .withSchemaName(applicationProperties.getName()))
              .withDwcRelatedResourceID(
                  applicationProperties.getSpecimenBaseUrl() + digitalSpecimenPid));
      var digitalMediaObjectEvent = new DigitalMediaEvent(
          digitalMediaObjectEventWithoutDoi.enrichmentList(),
          new DigitalMediaWrapper(
              digitalMediaObjectEventWithoutDoi.digitalMediaObjectWithoutDoi().type(),
              digitalSpecimenPid,
              attributes
          )
      );
      try {
        kafkaService.publishDigitalMediaObject(digitalMediaObjectEvent);
      } catch (JsonProcessingException e) {
        log.warn("Failed to publish digitalMediaEvent: {} for specimen {}",
            digitalMediaObjectEvent.digitalMediaWrapper().attributes().getAcAccessURI(),
            digitalSpecimenPid);
      }
    }
  }

  private void gatherDigitalMediaObjectForUpdatedRecords(
      Set<UpdatedDigitalSpecimenRecord> digitalSpecimenRecords) {
    digitalSpecimenRecords.forEach(digitalSpecimenRecord -> {
      var digitalSpecimenPid = digitalSpecimenRecord.digitalSpecimenRecord().id();
      var digitalMedia = digitalSpecimenRecord.digitalMediaObjectEvents();
      publishDigitalMediaRecord(digitalMedia, digitalSpecimenPid);
    });
  }

  private Map<String, String> createNewPidRecords(List<DigitalSpecimenEvent> events)
      throws PidCreationException, PidAuthenticationException {
    var specimenList = events.stream().map(DigitalSpecimenEvent::digitalSpecimenWrapper).toList();
    var request = fdoRecordService.buildPostHandleRequest(specimenList);
    return handleComponent.postHandle(request);
  }

  private void pidCreationFailed(List<DigitalSpecimenEvent> events) {
    rollbackHandlesFromPhysId(events);
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

  private void rollbackHandlesFromPhysId(List<DigitalSpecimenEvent> events) {
    var physIds = events.stream().map(DigitalSpecimenEvent::digitalSpecimenWrapper)
        .map(DigitalSpecimenWrapper::physicalSpecimenID).toList();
    handleComponent.rollbackFromPhysId(physIds);
  }

  private void handleSuccessfulElasticInsert(
      Map<DigitalSpecimenRecord, Pair<List<String>, List<DigitalMediaEventWithoutDOI>>> digitalSpecimenRecords) {
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

  private boolean publishEvents(DigitalSpecimenRecord key,
      Pair<List<String>, List<DigitalMediaEventWithoutDOI>> additionalInfo) {
    try {
      kafkaService.publishCreateEvent(key);
    } catch (JsonProcessingException e) {
      log.error("Rolling back, failed to publish Create event", e);
      rollbackNewSpecimen(key, additionalInfo, true);
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

  private void rollbackNewSpecimen(DigitalSpecimenRecord digitalSpecimenRecord,
      Pair<List<String>, List<DigitalMediaEventWithoutDOI>> additionalInfo) {
    rollbackNewSpecimen(digitalSpecimenRecord, additionalInfo, false);
  }

  private void rollbackNewSpecimen(DigitalSpecimenRecord digitalSpecimenRecord,
      Pair<List<String>, List<DigitalMediaEventWithoutDOI>> additionalInfo,
      boolean elasticRollback) {
    if (elasticRollback) {
      try {
        elasticRepository.rollbackSpecimen(digitalSpecimenRecord);
      } catch (IOException | ElasticsearchException e) {
        log.error("Fatal exception, unable to roll back: " + digitalSpecimenRecord.id(), e);
      }
    }
    repository.rollbackSpecimen(digitalSpecimenRecord.id());
    try {
      kafkaService.deadLetterEvent(
          new DigitalSpecimenEvent(additionalInfo.getLeft(),
              digitalSpecimenRecord.digitalSpecimenWrapper(),
              additionalInfo.getRight()));
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
        .filter(
            r -> fdoRecordService.handleNeedsUpdate(r.currentSpecimen().digitalSpecimenWrapper(),
                r.digitalSpecimenEvent().digitalSpecimenWrapper()))
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
    var handle = pidMap.get(event.digitalSpecimenWrapper().physicalSpecimenID());
    if (handle == null) {
      try {
        log.error("handle not created for Digital Specimen {}",
            event.digitalSpecimenWrapper().physicalSpecimenID());
        kafkaService.deadLetterEvent(event);
      } catch (JsonProcessingException e) {
        log.error("Kafka DLQ failed for specimen {}",
            event.digitalSpecimenWrapper().physicalSpecimenID());
      }
      return null;
    }

    return new DigitalSpecimenRecord(
        pidMap.get(event.digitalSpecimenWrapper().physicalSpecimenID()),
        midsService.calculateMids(event.digitalSpecimenWrapper()),
        1,
        Instant.now(),
        event.digitalSpecimenWrapper()
    );
  }
}

