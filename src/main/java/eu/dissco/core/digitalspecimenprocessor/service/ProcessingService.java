package eu.dissco.core.digitalspecimenprocessor.service;

import static eu.dissco.core.digitalspecimenprocessor.configuration.ApplicationConfiguration.DATE_STRING;
import static eu.dissco.core.digitalspecimenprocessor.domain.AgentRoleType.PROCESSING_SERVICE;
import static eu.dissco.core.digitalspecimenprocessor.domain.EntityRelationshipType.HAS_MEDIA;
import static eu.dissco.core.digitalspecimenprocessor.domain.EntityRelationshipType.HAS_SPECIMEN;
import static eu.dissco.core.digitalspecimenprocessor.schema.Agent.Type.SCHEMA_SOFTWARE_APPLICATION;
import static eu.dissco.core.digitalspecimenprocessor.schema.Identifier.DctermsType.DOI;
import static eu.dissco.core.digitalspecimenprocessor.util.DigitalSpecimenUtils.DOI_PREFIX;

import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.fge.jsonpatch.diff.JsonDiff;
import com.nimbusds.jose.util.Pair;
import eu.dissco.core.digitalspecimenprocessor.domain.ProcessResult;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaEvent;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaEventWithoutDOI;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaKey;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaProcessResult;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaWrapper;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenEvent;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenRecord;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenWrapper;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.UpdatedDigitalSpecimenRecord;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.UpdatedDigitalSpecimenTuple;
import eu.dissco.core.digitalspecimenprocessor.exception.DisscoRepositoryException;
import eu.dissco.core.digitalspecimenprocessor.exception.PidException;
import eu.dissco.core.digitalspecimenprocessor.property.ApplicationProperties;
import eu.dissco.core.digitalspecimenprocessor.repository.DigitalSpecimenRepository;
import eu.dissco.core.digitalspecimenprocessor.repository.ElasticSearchRepository;
import eu.dissco.core.digitalspecimenprocessor.schema.DigitalSpecimen;
import eu.dissco.core.digitalspecimenprocessor.schema.EntityRelationship;
import eu.dissco.core.digitalspecimenprocessor.util.AgentUtils;
import eu.dissco.core.digitalspecimenprocessor.web.HandleComponent;
import java.io.IOException;
import java.net.URI;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jooq.exception.DataAccessException;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class ProcessingService {

  private static final String DOI_STRING = "https://doi.org/";
  private static final String DLQ_FAILED = "Fatal exception, unable to dead letter queue: {}";
  private final DateTimeFormatter formatter = DateTimeFormatter.ofPattern(DATE_STRING)
      .withZone(ZoneOffset.UTC);
  private final DigitalSpecimenRepository repository;
  private final FdoRecordService fdoRecordService;
  private final ElasticSearchRepository elasticRepository;
  private final KafkaPublisherService kafkaService;
  private final MidsService midsService;
  private final HandleComponent handleComponent;
  private final ApplicationProperties applicationProperties;
  private final AnnotationPublisherService annotationPublisherService;
  private final ObjectMapper mapper;
  private final DigitalMediaService digitalMediaService;
  private final RollbackService rollbackService;

  private static void setGeneratedTimestampToNull(DigitalSpecimenWrapper currentDigitalSpecimen,
      DigitalSpecimenWrapper digitalSpecimen) {
    currentDigitalSpecimen.attributes().setDctermsModified(null);
    digitalSpecimen.attributes().setDctermsModified(null);
    currentDigitalSpecimen.attributes().setDctermsCreated(null);
    digitalSpecimen.attributes().setDctermsCreated(null);
  }

  public List<DigitalSpecimenRecord> handleMessages(List<DigitalSpecimenEvent> events) {
    log.info("Processing {} digital specimen", events.size());
    var uniqueBatch = removeDuplicatesInBatch(events);
    var processResult = processSpecimens(uniqueBatch);
    log.info("Batch consists of: {} new, {} update and {} equal specimen",
        processResult.newSpecimens().size(),
        processResult.changedSpecimens().size(),
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
      var mediaEvents = events.stream().map(DigitalSpecimenEvent::digitalMediaEvents)
          .flatMap(List::stream).toList();
      var existingMediaProcessResult = currentSpecimens.isEmpty() ?
          new HashMap<String, DigitalMediaProcessResult>() :
          digitalMediaService.getExistingDigitalMedia(currentSpecimens, mediaEvents);
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
          var digitalMediaProcessResult = existingMediaProcessResult.get(
              currentDigitalSpecimen.id());
          if (isEqual(currentDigitalSpecimen.digitalSpecimenWrapper(), digitalSpecimenWrapper)
              && digitalMediaProcessResult.newMedia().isEmpty()
              && digitalMediaProcessResult.tombstoneMedia().isEmpty()) {
            log.debug("Received digital specimen is equal to digital specimen: {}",
                currentDigitalSpecimen.id());
            equalSpecimens.add(currentDigitalSpecimen);
          } else {
            log.debug("Specimen with id: {} has received an update", currentDigitalSpecimen.id());
            changedSpecimens.add(
                new UpdatedDigitalSpecimenTuple(currentDigitalSpecimen, event,
                    digitalMediaProcessResult));
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
  We take over the ERDate from the current entity relationship if the ER is equal.

  To establish equality, we only compare type and attributes, not original data or
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
    var currentCreated = currentDigitalSpecimen.attributes().getDctermsCreated();
    setGeneratedTimestampToNull(currentDigitalSpecimen, digitalSpecimen);
    setTimestampsEntityRelationships(digitalSpecimen.attributes().getOdsHasEntityRelationships(),
        currentDigitalSpecimen.attributes().getOdsHasEntityRelationships());
    var mediaRelationships = removeMediaEntityRelationships(currentDigitalSpecimen.attributes());
    verifyOriginalData(currentDigitalSpecimen, digitalSpecimen);
    if (currentDigitalSpecimen.type().equals(digitalSpecimen.type()) &&
        currentDigitalSpecimen.attributes().equals(digitalSpecimen.attributes())) {
      digitalSpecimen.attributes().setDctermsModified(currentModified);
      currentDigitalSpecimen.attributes().setDctermsModified(currentModified);
      currentDigitalSpecimen.attributes().setDctermsCreated(currentCreated);
      digitalSpecimen.attributes().setDctermsCreated(currentCreated);
      var ers = new ArrayList<>(currentDigitalSpecimen.attributes().getOdsHasEntityRelationships());
      ers.addAll(mediaRelationships);
      currentDigitalSpecimen.attributes().setOdsHasEntityRelationships(ers);
      return true;
    } else {
      digitalSpecimen.attributes().setDctermsModified(formatter.format(Instant.now()));
      currentDigitalSpecimen.attributes().setDctermsCreated(currentCreated);
      digitalSpecimen.attributes().setDctermsCreated(currentCreated);
      currentDigitalSpecimen.attributes().setDctermsModified(currentModified);
      var mediaRelations = new ArrayList<>(
          digitalSpecimen.attributes().getOdsHasEntityRelationships());
      mediaRelations.addAll(mediaRelationships);
      currentDigitalSpecimen.attributes().setOdsHasEntityRelationships(mediaRelations);
      return false;
    }
  }

  /*
When all fields are equal except the timestamp we assume tha relationships are equal and the
timestamp can be taken over from the current entity relationship.
This will reduce the amount of updates and will only update the ER timestamp when there was an
actual change
*/
  private void setTimestampsEntityRelationships(List<EntityRelationship> entityRelationships,
      List<EntityRelationship> currentEntityRelationships) {
    for (var entityRelationship : entityRelationships) {
      for (var currentEntityrelationship : currentEntityRelationships) {
        if (Objects.equals(entityRelationship.getId(), currentEntityrelationship.getId()) &&
            Objects.equals(entityRelationship.getType(), currentEntityrelationship.getType()) &&
            Objects.equals(entityRelationship.getDwcRelationshipOfResource(),
                currentEntityrelationship.getDwcRelationshipOfResource()) &&
            Objects.equals(entityRelationship.getDwcRelationshipOfResourceID(),
                currentEntityrelationship.getDwcRelationshipOfResourceID()) &&
            Objects.equals(entityRelationship.getDwcRelatedResourceID(),
                currentEntityrelationship.getDwcRelatedResourceID()) &&
            Objects.equals(entityRelationship.getOdsRelatedResourceURI(),
                currentEntityrelationship.getOdsRelatedResourceURI()) &&
            Objects.equals(entityRelationship.getOdsHasAgents(),
                currentEntityrelationship.getOdsHasAgents()) &&
            Objects.equals(entityRelationship.getDwcRelationshipRemarks(),
                currentEntityrelationship.getDwcRelationshipRemarks())
        ) {
          entityRelationship.setDwcRelationshipEstablishedDate(
              currentEntityrelationship.getDwcRelationshipEstablishedDate());
        }
      }
    }
  }

  private List<EntityRelationship> removeMediaEntityRelationships(DigitalSpecimen attributes) {
    var mediaEntityRelationships = new ArrayList<EntityRelationship>();
    var remainingEntityRelationships = new ArrayList<EntityRelationship>();
    attributes.getOdsHasEntityRelationships().forEach(entityRelationship -> {
      if (entityRelationship.getDwcRelationshipOfResource().equals(HAS_MEDIA.getName())) {
        mediaEntityRelationships.add(entityRelationship);
      } else {
        remainingEntityRelationships.add(entityRelationship);
      }
    });
    attributes.setOdsHasEntityRelationships(remainingEntityRelationships);
    return mediaEntityRelationships;
  }

  private void verifyOriginalData(DigitalSpecimenWrapper currentDigitalSpecimen,
      DigitalSpecimenWrapper digitalSpecimen) {
    var currentOriginalData = currentDigitalSpecimen.originalAttributes();
    var originalData = digitalSpecimen.originalAttributes();
    if (currentOriginalData != null && !currentOriginalData.equals(originalData)) {
      log.info(
          "Original data for specimen with physical id {} has changed. Ignoring new original data.",
          digitalSpecimen.physicalSpecimenID());
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
    // Events contain the incoming media information, the record contains the id. We need to link both
    // to publish media events
    for (var event : events) {
      if (pidMap.containsKey(event.digitalSpecimenWrapper().physicalSpecimenID())) {
        var digitalSpecimenPid = pidMap.get(event.digitalSpecimenWrapper().physicalSpecimenID());
        var digitalMedia = event.digitalMediaEvents();
        publishDigitalMediaRecord(digitalMedia, digitalSpecimenPid, null);
      }
    }
  }

  private Set<DigitalSpecimenRecord> updateExistingDigitalSpecimen(
      List<UpdatedDigitalSpecimenTuple> updatedDigitalSpecimenTuples) {
    log.info("Persisting to Handle Server");
    var successfullyUpdatedHandles = updateHandles(updatedDigitalSpecimenTuples);
    if (!successfullyUpdatedHandles) {
      return Set.of();
    }
    var mediaPidMap = createMediaPidsForUpdatedRecords(
        updatedDigitalSpecimenTuples);
    var digitalSpecimenRecords = getSpecimenRecordMap(updatedDigitalSpecimenTuples, mediaPidMap);
    log.info("Persisting {} updated record to the database", digitalSpecimenRecords.size());
    try {
      repository.createDigitalSpecimenRecord(
          digitalSpecimenRecords.stream().map(UpdatedDigitalSpecimenRecord::digitalSpecimenRecord)
              .toList());
    } catch (DataAccessException e) {
      log.error("Unable to update records into database. Rolling back updates", e);
      rollbackService.rollbackUpdatedSpecimens(digitalSpecimenRecords, mediaPidMap, false, false);
      return Collections.emptySet();
    }
    digitalMediaService.removeSpecimenRelationshipsFromMedia(digitalSpecimenRecords);
    log.info("Persisting {} updated records to elastic", digitalSpecimenRecords.size());
    try {
      var bulkResponse = elasticRepository.indexDigitalSpecimen(
          digitalSpecimenRecords.stream().map(UpdatedDigitalSpecimenRecord::digitalSpecimenRecord)
              .toList());
      if (!bulkResponse.errors()) {
        handleSuccessfulElasticUpdate(digitalSpecimenRecords, mediaPidMap);
      } else {
        digitalSpecimenRecords = rollbackService.handlePartiallyFailedElasticUpdate(
            digitalSpecimenRecords, mediaPidMap, bulkResponse);
      }
      var successfullyProcessedRecords = digitalSpecimenRecords.stream()
          .map(UpdatedDigitalSpecimenRecord::digitalSpecimenRecord).collect(
              Collectors.toSet());
      log.info("Successfully updated {} digitalSpecimen records",
          successfullyProcessedRecords.size());
      annotationPublisherService.publishAnnotationUpdatedSpecimen(digitalSpecimenRecords);
      gatherDigitalMediaObjectForUpdatedRecords(digitalSpecimenRecords);
      return successfullyProcessedRecords;
    } catch (IOException | ElasticsearchException e) {
      log.error("Rolling back, failed to insert records in elastic", e);
      rollbackService.rollbackUpdatedSpecimens(digitalSpecimenRecords, mediaPidMap, false, true);
      return Set.of();
    }
  }

  private void handleSuccessfulElasticUpdate(
      Set<UpdatedDigitalSpecimenRecord> digitalSpecimenRecords,
      Map<DigitalMediaKey, String> mediaPidMap) {
    log.debug("Successfully indexed {} specimens", digitalSpecimenRecords);
    var failedRecords = new HashSet<UpdatedDigitalSpecimenRecord>();
    for (var digitalSpecimenRecord : digitalSpecimenRecords) {
      var successfullyPublished = publishUpdateEvent(digitalSpecimenRecord);
      if (!successfullyPublished) {
        failedRecords.add(digitalSpecimenRecord);
      }
    }
    if (!failedRecords.isEmpty()) {
      rollbackService.rollbackUpdatedSpecimens(failedRecords, mediaPidMap, true, true);
    }
    digitalSpecimenRecords.removeAll(failedRecords);
  }

  private Set<UpdatedDigitalSpecimenRecord> getSpecimenRecordMap(
      List<UpdatedDigitalSpecimenTuple> updatedDigitalSpecimenTuples,
      Map<DigitalMediaKey, String> mediaPidMap) {
    return updatedDigitalSpecimenTuples.stream().map(tuple -> {
          var recordNoMediaEr = new DigitalSpecimenRecord(
              tuple.currentSpecimen().id(),
              midsService.calculateMids(tuple.digitalSpecimenEvent().digitalSpecimenWrapper()),
              tuple.currentSpecimen().version() + 1,
              Instant.now(),
              tuple.digitalSpecimenEvent().digitalSpecimenWrapper());
          var digitalSpecimenRecord = setMediaEntityRelationship(tuple.digitalMediaProcessResult(),
              recordNoMediaEr, mediaPidMap);
          return new UpdatedDigitalSpecimenRecord(
              digitalSpecimenRecord,
              tuple.digitalSpecimenEvent().enrichmentList(),
              tuple.currentSpecimen(),
              createJsonPatch(
                  tuple.currentSpecimen().digitalSpecimenWrapper().attributes(),
                  tuple.digitalSpecimenEvent().digitalSpecimenWrapper().attributes()),
              tuple.digitalSpecimenEvent().digitalMediaEvents(),
              tuple.digitalMediaProcessResult());
        }
    ).collect(Collectors.toSet());
  }

  private boolean updateHandles(List<UpdatedDigitalSpecimenTuple> updatedDigitalSpecimenTuples) {
    var digitalSpecimensToUpdate = updatedDigitalSpecimenTuples.stream()
        .filter(tuple -> fdoRecordService.handleNeedsUpdate(
            tuple.currentSpecimen().digitalSpecimenWrapper(),
            tuple.digitalSpecimenEvent().digitalSpecimenWrapper()))
        .toList();

    if (!digitalSpecimensToUpdate.isEmpty()) {
      try {
        log.info("Updating {} handle records", digitalSpecimensToUpdate.size());
        var requests = fdoRecordService.buildUpdateHandleRequest(digitalSpecimensToUpdate);
        handleComponent.updateHandle(requests);
      } catch (PidException e) {
        log.error("Unable to update Handle record. Not proceeding with update. ", e);
        try {
          for (var tuple : updatedDigitalSpecimenTuples) {
            kafkaService.deadLetterEvent(tuple.digitalSpecimenEvent());
          }
        } catch (JsonProcessingException jsonEx) {
          log.error(DLQ_FAILED, updatedDigitalSpecimenTuples, jsonEx);
        }
        return false;
      }
    }
    return true;
  }

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

  private Set<DigitalSpecimenRecord> createNewDigitalSpecimen(List<DigitalSpecimenEvent> events) {
    Map<String, String> pidMap;
    Map<DigitalMediaKey, String> mediaPidMap;
    try {
      pidMap = createNewPidRecords(events);
    } catch (PidException e) {
      log.error("Unable to create PID. {}", e.getMessage());
      rollbackService.pidCreationFailed(events);
      return Collections.emptySet();
    }
    var digitalSpecimenRecords = events.stream().collect(Collectors.toMap(
            event -> mapToDigitalSpecimenRecord(event, pidMap),
            event -> Pair.of(event.enrichmentList(), event.digitalMediaEvents()),
            (event1, event2) -> event1)).entrySet()
        .stream().filter(e -> e.getKey() != null)
        .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
    if (digitalSpecimenRecords.isEmpty()) {
      return Collections.emptySet();
    }
    mediaPidMap = createMediaPidsForNewRecords(digitalSpecimenRecords);
    digitalSpecimenRecords.remove(null);
    digitalSpecimenRecords = digitalSpecimenRecords.entrySet().stream()
        .collect(Collectors.toMap(
            e -> setMediaEntityRelationship(
                new DigitalMediaProcessResult(Collections.emptyList(), Collections.emptyList(),
                    e.getValue().getRight()), e.getKey(), mediaPidMap),
            Entry::getValue));

    log.info("Inserting {} new specimen into the database",
        digitalSpecimenRecords.size());
    try {
      repository.createDigitalSpecimenRecord(digitalSpecimenRecords.keySet());
    } catch (DataAccessException e) {
      log.error("Unable to insert new specimens into the database. Rolling back handles", e);
      rollbackService.rollbackNewSpecimens(digitalSpecimenRecords, mediaPidMap, false,
          false);
      return Collections.emptySet();
    }
    try {
      log.info("Inserting {} new specimen into the elastic search",
          digitalSpecimenRecords.size());
      var bulkResponse = elasticRepository.indexDigitalSpecimen(
          digitalSpecimenRecords.keySet());
      if (!bulkResponse.errors()) {
        handleSuccessfulElasticInsert(digitalSpecimenRecords, mediaPidMap);
      } else {
        digitalSpecimenRecords = rollbackService.handlePartiallyFailedElasticInsert(
            digitalSpecimenRecords, mediaPidMap, bulkResponse);
      }
      log.info("Successfully created {} new digitalSpecimenRecord",
          digitalSpecimenRecords.size());
      annotationPublisherService.publishAnnotationNewSpecimen(
          digitalSpecimenRecords.keySet());
      if (!mediaPidMap.isEmpty()) {
        gatherDigitalMediaObjectForNewRecords(digitalSpecimenRecords, mediaPidMap);
      }
      return digitalSpecimenRecords.keySet();
    } catch (IOException | ElasticsearchException e) {
      log.error("Rolling back, failed to insert records in elastic", e);
      rollbackService.rollbackNewSpecimens(digitalSpecimenRecords, mediaPidMap, false, true);
      return Collections.emptySet();
    }
  }

  private DigitalSpecimenRecord setMediaEntityRelationship(
      DigitalMediaProcessResult mediaProcessResult,
      DigitalSpecimenRecord digitalSpecimenRecord, Map<DigitalMediaKey, String> mediaPidMap) {
    var newEntityRelationshipStream = mediaProcessResult.newMedia().stream()
        .map(mediaEvent -> {
          var digitalMediaKey = new DigitalMediaKey(digitalSpecimenRecord.id(),
              mediaEvent.digitalMediaObjectWithoutDoi().attributes().getAcAccessURI());
          return buildEntityRelationship(HAS_MEDIA.getName(), mediaPidMap.get(
              digitalMediaKey));
        });
    var totalErs = Stream.concat(
        digitalSpecimenRecord.digitalSpecimenWrapper().attributes().getOdsHasEntityRelationships()
            .stream()
            // Filters out tombstoned media relations so we don't include then in the new version
            .filter(entityRelationship -> !mediaProcessResult.tombstoneMedia()
                .contains(entityRelationship)),
        newEntityRelationshipStream).toList();
    var existingAttributes = digitalSpecimenRecord.digitalSpecimenWrapper().attributes();
    existingAttributes.setOdsHasEntityRelationships(totalErs);
    return new DigitalSpecimenRecord(
        digitalSpecimenRecord.id(),
        digitalSpecimenRecord.midsLevel(),
        digitalSpecimenRecord.version(),
        digitalSpecimenRecord.created(),
        new DigitalSpecimenWrapper(
            digitalSpecimenRecord.digitalSpecimenWrapper().physicalSpecimenID(),
            digitalSpecimenRecord.digitalSpecimenWrapper().type(),
            existingAttributes,
            digitalSpecimenRecord.digitalSpecimenWrapper().originalAttributes()
        )
    );
  }

  private void gatherDigitalMediaObjectForNewRecords(
      Map<DigitalSpecimenRecord, Pair<List<String>, List<DigitalMediaEventWithoutDOI>>> digitalSpecimenRecords,
      Map<DigitalMediaKey, String> mediaPids) {
    log.info("Publishing digital media object events for processing");
    digitalSpecimenRecords.forEach((key, value) -> {
      var digitalSpecimenPid = key.id();
      var digitalMedia = value.getRight();
      publishDigitalMediaRecord(digitalMedia, digitalSpecimenPid, mediaPids);
    });
  }

  private void publishDigitalMediaRecord(List<DigitalMediaEventWithoutDOI> digitalMedia,
      String digitalSpecimenPid, Map<DigitalMediaKey, String> mediaPidMap) {
    for (var digitalMediaObjectEventWithoutDoi : digitalMedia) {
      var attributes = digitalMediaObjectEventWithoutDoi.digitalMediaObjectWithoutDoi()
          .attributes();
      var digitalMediaKey = new DigitalMediaKey(digitalSpecimenPid, attributes.getAcAccessURI());
      String mediaPid = mediaPidMap == null ? null :
          mediaPidMap.get(digitalMediaKey);
      attributes.getOdsHasEntityRelationships().add(
          buildEntityRelationship(HAS_SPECIMEN.getName(), digitalSpecimenPid));
      var digitalMediaObjectEvent = new DigitalMediaEvent(
          digitalMediaObjectEventWithoutDoi.enrichmentList(),
          new DigitalMediaWrapper(
              digitalMediaObjectEventWithoutDoi.digitalMediaObjectWithoutDoi().type(),
              digitalSpecimenPid,
              attributes
                  .withId(mediaPid)
                  .withDctermsIdentifier(mediaPid),
              digitalMediaObjectEventWithoutDoi.digitalMediaObjectWithoutDoi().originalAttributes())
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

  private EntityRelationship buildEntityRelationship(String relationshipType,
      String relatedResourceId) {
    return new EntityRelationship()
        .withType("ods:EntityRelationship")
        .withDwcRelationshipEstablishedDate(Date.from(Instant.now()))
        .withDwcRelationshipOfResource(relationshipType)
        .withOdsHasAgents(List.of(AgentUtils.createMachineAgent(applicationProperties.getName(),
            applicationProperties.getPid(), PROCESSING_SERVICE, DOI, SCHEMA_SOFTWARE_APPLICATION)))
        .withDwcRelatedResourceID(DOI_PREFIX + relatedResourceId)
        .withOdsRelatedResourceURI(URI.create(DOI_PREFIX + relatedResourceId));
  }

  private void gatherDigitalMediaObjectForUpdatedRecords(
      Set<UpdatedDigitalSpecimenRecord> digitalSpecimenRecords) {
    digitalSpecimenRecords.forEach(digitalSpecimenRecord -> {
      var digitalSpecimenPid = digitalSpecimenRecord.digitalSpecimenRecord().id();
      var digitalMedia = digitalSpecimenRecord.digitalMediaObjectEvents();
      publishDigitalMediaRecord(digitalMedia, digitalSpecimenPid, null);
    });
  }

  private Map<String, String> createNewPidRecords(List<DigitalSpecimenEvent> events)
      throws PidException {
    var specimenList = events.stream().map(DigitalSpecimenEvent::digitalSpecimenWrapper).toList();
    var request = fdoRecordService.buildPostHandleRequest(specimenList);
    if (!request.isEmpty()) {
      return handleComponent.postHandle(request);
    } else {
      return Map.of();
    }

  }

  private Map<DigitalMediaKey, String> createMediaPidsForNewRecords(
      Map<DigitalSpecimenRecord, Pair<List<String>, List<DigitalMediaEventWithoutDOI>>> digitalSpecimenRecords) {
    var specimenMediaPairs = digitalSpecimenRecords.entrySet().stream()
        .filter(specimenRecord -> specimenRecord.getKey().digitalSpecimenWrapper().attributes()
            .getOdsIsKnownToContainMedia())
        .map(e -> Pair.of(e.getKey().id(),
            e.getValue().getRight())) // specimen id -> digitalMediaRecords
        .toList();
    if (specimenMediaPairs.isEmpty()) {
      return Collections.emptyMap();
    }
    var requests = specimenMediaPairs.stream()
        .map(
            specimenMediaPair -> fdoRecordService.buildPostRequestMedia(specimenMediaPair.getLeft(),
                specimenMediaPair.getRight()))
        .flatMap(List::stream)
        .toList();
    if (requests.isEmpty()) {
      return Collections.emptyMap();
    }
    return postMediaHandles(requests);
  }

  private Map<DigitalMediaKey, String> createMediaPidsForUpdatedRecords(
      List<UpdatedDigitalSpecimenTuple> updatedDigitalSpecimenTuples) {
    if (updatedDigitalSpecimenTuples.stream()
        .map(updateTuple -> updateTuple.digitalMediaProcessResult().newMedia())
        .flatMap(List::stream).toList()
        .isEmpty()) {
      return Collections.emptyMap();
    }
    var requests = updatedDigitalSpecimenTuples.stream()
        .map(updateTuples -> fdoRecordService.buildPostRequestMedia(
            updateTuples.currentSpecimen().id(),
            updateTuples.digitalMediaProcessResult().newMedia()))
        .flatMap(List::stream)
        .toList();
    return postMediaHandles(requests);
  }

  private Map<DigitalMediaKey, String> postMediaHandles(List<JsonNode> requests) {
    try {
      log.info("Minting PIDs for media");
      return handleComponent.postMediaHandle(requests);
    } catch (PidException e) {
      log.error(
          "PID creation for Media objects failed. Media objects for this batch will not be published",
          e);
      return Collections.emptyMap();
    }
  }

  private void handleSuccessfulElasticInsert(
      Map<DigitalSpecimenRecord, Pair<List<String>, List<DigitalMediaEventWithoutDOI>>> digitalSpecimenRecords,
      Map<DigitalMediaKey, String> mediaPidMap) {
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
      rollbackService.rollbackNewSpecimens(digitalSpecimenRecords, mediaPidMap, true, true);
    }
  }


  private boolean publishEvents(DigitalSpecimenRecord key,
      Pair<List<String>, List<DigitalMediaEventWithoutDOI>> additionalInfo) {
    try {
      kafkaService.publishCreateEvent(key);
    } catch (JsonProcessingException e) {
      log.error("Rolling back, failed to publish Create event", e);
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

  // We remove the dcterms:modified from the comparison as it is generated and will always differ
  private JsonNode createJsonPatch(DigitalSpecimen currentDigitalSpecimen,
      DigitalSpecimen digitalSpecimen) {
    var jsonCurrentSpecimen = (ObjectNode) mapper.valueToTree(currentDigitalSpecimen);
    var jsonSpecimen = (ObjectNode) mapper.valueToTree(digitalSpecimen);
    jsonCurrentSpecimen.set("dcterms:modified", null);
    jsonSpecimen.set("dcterms:modified", null);
    return JsonDiff.asJson(jsonCurrentSpecimen, jsonSpecimen);
  }
}

