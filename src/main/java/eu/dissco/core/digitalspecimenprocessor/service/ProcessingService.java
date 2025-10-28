package eu.dissco.core.digitalspecimenprocessor.service;

import static eu.dissco.core.digitalspecimenprocessor.util.DigitalObjectUtils.DOI_PROXY;
import static java.util.stream.Collectors.toMap;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaEvent;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaRecord;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaWrapper;
import eu.dissco.core.digitalspecimenprocessor.domain.media.MediaPreprocessResult;
import eu.dissco.core.digitalspecimenprocessor.domain.media.MediaProcessResult;
import eu.dissco.core.digitalspecimenprocessor.domain.media.UpdatedDigitalMediaTuple;
import eu.dissco.core.digitalspecimenprocessor.domain.relation.DigitalMediaRelationshipTombstoneEvent;
import eu.dissco.core.digitalspecimenprocessor.domain.relation.MediaRelationshipProcessResult;
import eu.dissco.core.digitalspecimenprocessor.domain.relation.PidProcessResult;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenEvent;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenRecord;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.SpecimenPreprocessResult;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.SpecimenProcessResult;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.UpdatedDigitalSpecimenTuple;
import eu.dissco.core.digitalspecimenprocessor.exception.DisscoRepositoryException;
import eu.dissco.core.digitalspecimenprocessor.exception.JsonMappingException;
import eu.dissco.core.digitalspecimenprocessor.exception.PidException;
import eu.dissco.core.digitalspecimenprocessor.exception.TooManyObjectsException;
import eu.dissco.core.digitalspecimenprocessor.property.ApplicationProperties;
import eu.dissco.core.digitalspecimenprocessor.repository.DigitalMediaRepository;
import eu.dissco.core.digitalspecimenprocessor.repository.DigitalSpecimenRepository;
import eu.dissco.core.digitalspecimenprocessor.schema.DigitalMedia;
import eu.dissco.core.digitalspecimenprocessor.schema.EntityRelationship;
import eu.dissco.core.digitalspecimenprocessor.web.HandleComponent;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class ProcessingService {

  private final ObjectMapper objectMapper;
  private final DigitalSpecimenRepository repository;
  private final DigitalMediaRepository mediaRepository;
  private final DigitalSpecimenService digitalSpecimenService;
  private final DigitalMediaService digitalMediaService;
  private final EntityRelationshipService entityRelationshipService;
  private final EqualityService equalityService;
  private final RabbitMqPublisherService publisherService;
  private final FdoRecordService fdoRecordService;
  private final HandleComponent handleComponent;
  private final ApplicationProperties applicationProperties;
  private final MasSchedulerService masSchedulerService;

  private static Map<String, PidProcessResult> updateMediaPidsWithResults(
      SpecimenProcessResult specimenResult, SpecimenPreprocessResult specimenPreprocessResult,
      Map<String, PidProcessResult> mediaPidsFull) {
    if ((specimenResult.updatedDigitalSpecimens().size() + specimenPreprocessResult.newSpecimens()
        .size()) < (specimenPreprocessResult.changedSpecimens().size()
        + specimenPreprocessResult.newSpecimens().size())) {
      return mediaPidsFull;
    }
    // If we had a partial success, and not all specimens were created, we don't want to create meaningless ERS on our media
    // So we filter out the specimen PIDs that were not in our results
    var changedSpecimens = Stream.concat(specimenResult.updatedDigitalSpecimens().stream(),
            specimenResult.newDigitalSpecimens().stream())
        .toList();
    var specimenDOIs = changedSpecimens.stream().map(DigitalSpecimenRecord::id).toList();
    var mediaPidsFiltered = new HashMap<String, PidProcessResult>();
    for (var mediaPid : mediaPidsFull.entrySet()) {
      var relatedDois = mediaPid.getValue().doisOfRelatedObjects().stream().filter(
          specimenDOIs::contains
      ).collect(Collectors.toSet());
      mediaPidsFiltered.put(mediaPid.getKey(),
          new PidProcessResult(mediaPid.getValue().doiOfTarget(), relatedDois));
    }
    return mediaPidsFiltered;
  }

  // Given a specimen PID, links it to the relevant media object
  private static void updateMediaHashMap(HashMap<String, HashSet<String>> mediaHashMap,
      Set<String> mediaPidsForThisSpecimen, Map<String, String> allMediaPids, String specimenPid) {
    if (mediaPidsForThisSpecimen.isEmpty()) {
      return;
    }
    allMediaPids.entrySet()
        .stream()
        .filter(e -> mediaPidsForThisSpecimen.contains(e.getValue())) // Only look at relevant pids
        .forEach(e -> {
          var uri = e.getKey();
          mediaHashMap.computeIfAbsent(uri, k -> new HashSet<>()).add(specimenPid);
        });
  }

  private static Map<String, String> concatSpecimenPids(
      SpecimenPreprocessResult specimenPreprocessResult) {
    var existingSpecimenPids = Stream.concat(
        specimenPreprocessResult.equalSpecimens().stream(),
        specimenPreprocessResult.changedSpecimens().stream()
            .map(UpdatedDigitalSpecimenTuple::currentSpecimen)
    ).collect(toMap(
        specimen -> specimen.digitalSpecimenWrapper().physicalSpecimenID(),
        DigitalSpecimenRecord::id
    ));
    return concatMaps(specimenPreprocessResult.newSpecimenPids(), existingSpecimenPids);
  }

  private static Map<String, String> concatMediaPids(
      Map<String, DigitalMediaRecord> existingMedias, Map<String, String> newMediaPids) {
    var existingPidMap = existingMedias.entrySet()
        .stream().collect(toMap(
            Entry::getKey,
            e -> e.getValue().id()
        ));
    return concatMaps(existingPidMap, newMediaPids);
  }

  private static Map<String, String> concatMaps(Map<String, String> m1, Map<String, String> m2) {
    return Stream.concat(
        m1.entrySet().stream(), m2.entrySet().stream()
    ).collect(toMap(
        Entry::getKey,
        Entry::getValue
    ));
  }


  private static List<EntityRelationship> removeRelationship(
      DigitalMediaRelationshipTombstoneEvent event,
      DigitalMedia updatedMedia) {
    var newEntityRelationships = new ArrayList<EntityRelationship>();
    updatedMedia.getOdsHasEntityRelationships().stream().filter(
            er -> !er.getOdsRelatedResourceURI().toString().equals(DOI_PROXY + event.specimenDoi()))
        .forEach(newEntityRelationships::add);
    return newEntityRelationships;
  }

  private static void addToUniqueSets(LinkedHashSet<DigitalSpecimenEvent> uniqueSet,
      DigitalSpecimenEvent entry, HashSet<String> uniqueMediaSet) {
    uniqueSet.add(entry);
    uniqueMediaSet.addAll(entry.digitalMediaEvents().stream()
        .map(e -> e.digitalMediaWrapper().attributes().getAcAccessURI())
        .toList());
  }

  private static boolean checkIfMediaIsUnique(DigitalSpecimenEvent entry,
      HashSet<String> uniqueMediaSet) {
    return entry.digitalMediaEvents().stream()
        .map(e -> e.digitalMediaWrapper().attributes().getAcAccessURI())
        .noneMatch(uniqueMediaSet::contains);
  }

  public SpecimenProcessResult handleMessages(List<DigitalSpecimenEvent> events) {
    log.info("Processing {} digital specimen", events.size());
    try {
      var validEvents = checkImageDuplicationSingleSpecimen(events);
      var uniqueBatchSpecimens = removeDuplicatesInBatch(validEvents);
      if (uniqueBatchSpecimens.isEmpty()) {
        log.warn("After removing any non-complaint events, there is nothing to process");
        return new SpecimenProcessResult();
      }
      var uniqueBatchMedia = getUniqueDigitalMediaEvents(uniqueBatchSpecimens);
      var existingSpecimens = getCurrentSpecimen(uniqueBatchSpecimens);
      var existingMedia = getCurrentMedia(uniqueBatchMedia);
      var specimenPreprocessResult = preprocessSpecimens(uniqueBatchSpecimens, existingSpecimens,
          existingMedia);
      var pids = processPids(specimenPreprocessResult, existingMedia, uniqueBatchSpecimens,
          uniqueBatchMedia);
      var mediaPreprocessResult = preprocessMedia(uniqueBatchMedia, existingMedia, pids.getRight());
      log.info("Batch consists of: {} new, {} update, and {} equal specimens",
          specimenPreprocessResult.newSpecimens().size(),
          specimenPreprocessResult.changedSpecimens().size(),
          specimenPreprocessResult.equalSpecimens().size());
      log.info("Batch consists of {} new, {} update, and {} equal media",
          mediaPreprocessResult.newDigitalMedia().size(),
          mediaPreprocessResult.changedDigitalMedia().size(),
          mediaPreprocessResult.equalDigitalMedia().size());
      var specimenResults = processSpecimens(specimenPreprocessResult, pids.getLeft());
      var mediaPids = updateMediaPidsWithResults(specimenResults, specimenPreprocessResult,
          pids.getRight());
      var mediaResults = processMedia(mediaPreprocessResult, mediaPids);
      log.info("Processed specimen and media");
      scheduleMas(specimenResults, mediaResults);
      return specimenResults;
    } catch (DisscoRepositoryException e) {
      log.error("Unable to access database", e);
      return new SpecimenProcessResult(List.of(), List.of(), List.of());
    }
  }

  private Set<DigitalMediaEvent> getUniqueDigitalMediaEvents(
      Set<DigitalSpecimenEvent> validEvents) {
    var uniqueBatchMedia = validEvents.stream()
        .flatMap(event -> event.digitalMediaEvents().stream())
        .collect(
            Collectors.toSet());
    if (uniqueBatchMedia.size() > applicationProperties.getMaxMedia()) {
      log.error("Too many media in batch. Attempting to publish {} media at once",
          uniqueBatchMedia.size());
      throw new TooManyObjectsException(
          "Attempting to publish too many media objects. Max is 10000");
    }
    return uniqueBatchMedia;
  }

  private List<DigitalSpecimenEvent> checkImageDuplicationSingleSpecimen(
      List<DigitalSpecimenEvent> events) {
    var validEvent = new ArrayList<DigitalSpecimenEvent>();
    for (var event : events) {
      if (event.digitalMediaEvents().stream()
          .collect(
              Collectors.groupingBy(e -> e.digitalMediaWrapper().attributes().getAcAccessURI()))
          .values()
          .stream()
          .anyMatch(e -> e.size() > 1)) {
        try {
          log.error(
              "Found duplicate media in single specimen event for specimen id: {}. Dead lettering event",
              event.digitalSpecimenWrapper().physicalSpecimenID());
          publisherService.deadLetterEventSpecimen(event);
        } catch (JsonProcessingException e) {
          log.error("Fatal exception, unable to dead letter queue: {}", event, e);
        }
      } else {
        validEvent.add(event);
      }
    }
    return validEvent;
  }

  private void scheduleMas(SpecimenProcessResult specimenResult,
      MediaProcessResult mediaProcessResult) {
    masSchedulerService.scheduleMasForSpecimen(specimenResult);
    masSchedulerService.scheduleMasForMedia(mediaProcessResult);
  }

  public MediaProcessResult handleMessagesMedia(List<DigitalMediaEvent> events) {
    var uniqueBatchMedia = removeDuplicateMediaInBatch(events);
    var existingMedia = getCurrentMedia(uniqueBatchMedia);
    var mediaPids = processMediaPids(existingMedia, uniqueBatchMedia);
    var mediaProcessResult = preprocessMedia(uniqueBatchMedia, existingMedia, mediaPids);
    var mediaResult = processMedia(mediaProcessResult, mediaPids);
    masSchedulerService.scheduleMasForMedia(mediaResult);
    return mediaResult;
  }

  /*
   * We need a way to map the specimen PIDs to the media and vice versa
   * Each has a many-to-many relationship
   * */
  private Pair<Map<String, PidProcessResult>, Map<String, PidProcessResult>> processPids(
      SpecimenPreprocessResult specimenPreprocessResult,
      Map<String, DigitalMediaRecord> existingMedias,
      Set<DigitalSpecimenEvent> digitalSpecimenEvents,
      Set<DigitalMediaEvent> digitalMediaEvents) {
    var specimenPids = new HashMap<String, PidProcessResult>();
    var mediaHashMap = new HashMap<String, HashSet<String>>(); // key = local id
    var allSpecimenPids = concatSpecimenPids(specimenPreprocessResult);
    var newMediaPids = createPidsForNewMediaObjects(existingMedias,
        digitalMediaEvents); // key = local id
    var allMediaPids = concatMediaPids(existingMedias, newMediaPids); // key = local id
    for (var specimen : digitalSpecimenEvents) {
      var mediaDoisForThisSpecimen = specimen.digitalMediaEvents().stream()
          .map(event -> event.digitalMediaWrapper().attributes().getAcAccessURI())
          .map(allMediaPids::get)
          .collect(Collectors.toSet());
      var specimenPid = allSpecimenPids.get(specimen.digitalSpecimenWrapper().physicalSpecimenID());
      specimenPids.put(specimen.digitalSpecimenWrapper().physicalSpecimenID(),
          new PidProcessResult(specimenPid, mediaDoisForThisSpecimen));
      // We record the specimen -> media relationship in an intermediate map
      // Multiple specimens may refer to the same media object
      // But the media event doesn't have a direct link to the specimens it refers to
      updateMediaHashMap(mediaHashMap, mediaDoisForThisSpecimen, allMediaPids, specimenPid);
    }
    var mediaPids = mediaHashMap.entrySet().stream()
        .collect(toMap(
            Entry::getKey,
            e -> new PidProcessResult(allMediaPids.get(e.getKey()), e.getValue()))
        );
    for (var mediaPid : mediaPids.entrySet()) {
      if (mediaPid.getValue().doisOfRelatedObjects().size() > 1) {
        log.info("Media {} has {} related specimens", mediaPid.getValue().doiOfTarget(),
            mediaPid.getValue().doisOfRelatedObjects().size());
      }
    }
    return Pair.of(specimenPids, mediaPids);
  }

  private Map<String, PidProcessResult> processMediaPids(
      Map<String, DigitalMediaRecord> existingMedias, Set<DigitalMediaEvent> digitalMediaEvents) {
    var mediaPidMap = createPidsForNewMediaObjects(existingMedias, digitalMediaEvents).entrySet()
        .stream()
        .collect(Collectors.toMap(
            Entry::getKey,
            e -> new PidProcessResult(e.getValue(), Set.of())
        ));
    mediaPidMap.putAll(
        existingMedias.entrySet().stream().collect(Collectors.toMap(
            Entry::getKey,
            e -> new PidProcessResult(e.getValue().id(), Set.of())
        )));
    return mediaPidMap;
  }

  private SpecimenProcessResult processSpecimens(
      SpecimenPreprocessResult specimenPreprocessResult,
      Map<String, PidProcessResult> pidProcessResults) {
    var equalSpecimens = new ArrayList<DigitalSpecimenRecord>();
    var updatedSpecimens = new ArrayList<DigitalSpecimenRecord>();
    var newSpecimens = new ArrayList<DigitalSpecimenRecord>();
    if (!specimenPreprocessResult.equalSpecimens().isEmpty()) {
      digitalSpecimenService.updateEqualSpecimen(specimenPreprocessResult.equalSpecimens());
      equalSpecimens = new ArrayList<>(specimenPreprocessResult.equalSpecimens());
    }
    if (!specimenPreprocessResult.newSpecimens().isEmpty()) {
      if (!specimenPreprocessResult.newSpecimenPids()
          .isEmpty()) {
        newSpecimens = new ArrayList<>(
            digitalSpecimenService.createNewDigitalSpecimen(specimenPreprocessResult.newSpecimens(),
                pidProcessResults));
      } else {
        log.warn("Unable to create new specimen pids for {} specimens. Ignoring new specimens",
            specimenPreprocessResult.newSpecimens().size());
      }
    }
    if (!specimenPreprocessResult.changedSpecimens().isEmpty()) {
      updatedSpecimens = new ArrayList<>(digitalSpecimenService.updateExistingDigitalSpecimen(
          specimenPreprocessResult.changedSpecimens(), pidProcessResults));
    }
    return new SpecimenProcessResult(equalSpecimens, updatedSpecimens, newSpecimens);
  }

  private MediaProcessResult processMedia(MediaPreprocessResult mediaPreprocessResult,
      Map<String, PidProcessResult> pidProcessResults) {
    var equalMedia = new ArrayList<DigitalMediaRecord>();
    var updatedMedia = new ArrayList<DigitalMediaRecord>();
    var newMedia = new ArrayList<DigitalMediaRecord>();
    if (!mediaPreprocessResult.equalDigitalMedia().isEmpty()) {
      digitalMediaService.updateEqualDigitalMedia(mediaPreprocessResult.equalDigitalMedia());
      equalMedia = new ArrayList<>(mediaPreprocessResult.equalDigitalMedia());
    }
    if (!mediaPreprocessResult.newDigitalMedia().isEmpty()) {
      newMedia = new ArrayList<>(
          digitalMediaService.createNewDigitalMedia(mediaPreprocessResult.newDigitalMedia(),
              pidProcessResults));
    }
    if (!mediaPreprocessResult.changedDigitalMedia().isEmpty()) {
      updatedMedia = new ArrayList<>(
          digitalMediaService.updateExistingDigitalMedia(
              mediaPreprocessResult.changedDigitalMedia(),
              true
          ));
    }
    return new MediaProcessResult(equalMedia, updatedMedia, newMedia);
  }


  private Set<DigitalSpecimenEvent> removeDuplicatesInBatch(
      List<DigitalSpecimenEvent> events) {
    var uniqueSet = new LinkedHashSet<DigitalSpecimenEvent>();
    var uniqueMediaSet = new LinkedHashSet<String>();
    var map = events.stream()
        .collect(
            Collectors.groupingBy(event -> event.digitalSpecimenWrapper().physicalSpecimenID()));
    for (var entry : map.entrySet()) {
      if (entry.getValue().size() > 1) {
        log.warn("Found {} duplicate specimen in batch for id {}", entry.getValue().size(),
            entry.getKey());
        var specimenIsNotPublished = true;
        for (var duplicateSpecimenEvent : entry.getValue()) {
          if (specimenIsNotPublished && checkIfMediaIsUnique(duplicateSpecimenEvent,
              uniqueMediaSet)) {
            addToUniqueSets(uniqueSet, duplicateSpecimenEvent, uniqueMediaSet);
            specimenIsNotPublished = false;
          } else {
            republishSpecimenEvent(duplicateSpecimenEvent);
          }
        }
      } else if (checkIfMediaIsUnique(entry.getValue().getFirst(), uniqueMediaSet)) {
        addToUniqueSets(uniqueSet, entry.getValue().getFirst(), uniqueMediaSet);
      } else {
        republishSpecimenEvent(entry.getValue().getFirst());
      }
    }
    return uniqueSet;
  }

  private Set<DigitalMediaEvent> removeDuplicateMediaInBatch(
      List<DigitalMediaEvent> mediaEvents) {
    var uniqueSet = new LinkedHashSet<DigitalMediaEvent>();
    var map = mediaEvents.stream()
        .collect(
            Collectors.groupingBy(
                event -> event.digitalMediaWrapper().attributes().getAcAccessURI(),
                Collectors.toCollection(LinkedHashSet::new)));
    for (var entry : map.entrySet()) {
      if (entry.getValue().size() > 1) {
        log.warn("Found {} duplicate media in batch for id {}", entry.getValue().size(),
            entry.getKey());
        boolean isFirst = true;
        for (var event : entry.getValue()) {
          if (isFirst) {
            uniqueSet.add(event);
            isFirst = false;
          } else {
            republishMediaEvent(event);
          }
        }
      } else {
        entry.getValue().stream().findFirst().ifPresent(uniqueSet::add);
      }
    }
    if (uniqueSet.size() > applicationProperties.getMaxMedia()) {
      log.error("Too many media in batch. Attempting to publish {} media at once",
          uniqueSet.size());
      throw new TooManyObjectsException(
          "Attempting to publish too many media objects. Max is 10000");
    }
    return uniqueSet;

  }

  private SpecimenPreprocessResult preprocessSpecimens(Set<DigitalSpecimenEvent> events,
      Map<String, DigitalSpecimenRecord> currentSpecimens,
      Map<String, DigitalMediaRecord> currentMedia) {
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
        MediaRelationshipProcessResult processedMediaRelationships;
        if (event.isDataFromSourceSystem().booleanValue()) {
          processedMediaRelationships = entityRelationshipService.processMediaRelationshipsForSpecimen(
              currentSpecimens, event, currentMedia);
        } else {
          processedMediaRelationships = new MediaRelationshipProcessResult();
        }
        if (equalityService.specimensAreEqual(currentDigitalSpecimen,
            digitalSpecimenWrapper, processedMediaRelationships)) {
          log.debug("Received digital specimen is equal to digital specimen: {}",
              currentDigitalSpecimen.id());
          equalSpecimens.add(currentDigitalSpecimen);
        } else {
          log.debug("Specimen with id: {} has received an update", currentDigitalSpecimen.id());
          var eventWithUpdatedEr = equalityService.setExistingEventDatesSpecimen(
              currentDigitalSpecimen.digitalSpecimenWrapper(), event, processedMediaRelationships);
          changedSpecimens.add(
              new UpdatedDigitalSpecimenTuple(currentDigitalSpecimen, eventWithUpdatedEr,
                  processedMediaRelationships));
        }
      }
    }
    var newSpecimenPids = createNewSpecimenPids(newSpecimens);
    return new SpecimenPreprocessResult(equalSpecimens, changedSpecimens, newSpecimens,
        newSpecimenPids);
  }

  private Map<String, String> createNewSpecimenPids(List<DigitalSpecimenEvent> events) {
    if (events.isEmpty()) {
      return Map.of();
    }
    var specimenList = events.stream().map(DigitalSpecimenEvent::digitalSpecimenWrapper).toList();
    var pidMap = new HashMap<String, String>();
    for (int i = 0; i < specimenList.size(); i += applicationProperties.getMaxHandles()) {
      int j = Math.min(i + applicationProperties.getMaxHandles(), specimenList.size());
      var sublist = specimenList.subList(i, j);
      var request = fdoRecordService.buildPostHandleRequest(sublist);
      pidMap.putAll(createNewPids(request, true));
    }
    return pidMap;
  }

  private Map<String, String> createPidsForNewMediaObjects(
      Map<String, DigitalMediaRecord> existingMedia,
      Set<DigitalMediaEvent> digitalMediaEvents) {
    var newEvents = digitalMediaEvents.stream()
        .filter(
            e -> !existingMedia.containsKey(e.digitalMediaWrapper().attributes().getAcAccessURI()))
        .toList();
    return createNewMediaPids(newEvents);
  }

  private Map<String, String> createNewMediaPids(
      List<DigitalMediaEvent> digitalMediaEvents) {
    if (digitalMediaEvents.isEmpty()) {
      return Map.of();
    }
    var pidMap = new HashMap<String, String>();
    for (int i = 0; i < digitalMediaEvents.size(); i += applicationProperties.getMaxHandles()) {
      int j = Math.min(i + applicationProperties.getMaxHandles(), digitalMediaEvents.size());
      var sublist = digitalMediaEvents.subList(i, j);
      var request = fdoRecordService.buildPostRequestMedia(sublist);
      pidMap.putAll(createNewPids(request, false));
    }
    return pidMap;
  }

  private Map<String, String> createNewPids(List<JsonNode> request, boolean isSpecimen) {
    try {
      var pidMap = handleComponent.postHandle(request, isSpecimen);
      log.info("Successfully minted {} {} PIDs", pidMap.size(), isSpecimen ? "specimen" : "media");
      return pidMap; // map localId : doiOfTarget
    } catch (PidException e) {
      log.error("Unable to create new PIDs", e);
    }
    return Map.of();
  }

  private MediaPreprocessResult preprocessMedia(Set<DigitalMediaEvent> events,
      Map<String, DigitalMediaRecord> currentDigitalMedias, Map<String, PidProcessResult> pidMap) {
    var equalDigitalMedia = new ArrayList<DigitalMediaRecord>();
    var changedDigitalMedia = new ArrayList<UpdatedDigitalMediaTuple>();
    var newDigitalMedia = new ArrayList<DigitalMediaEvent>();
    for (var mediaEvent : events) {
      var digitalMedia = mediaEvent.digitalMediaWrapper();
      var accessUri = digitalMedia.attributes().getAcAccessURI();
      log.debug("Processing digitalMediaWrapper: {}", digitalMedia);
      if (!currentDigitalMedias.containsKey(accessUri)) {
        log.debug("DigitalMedia with uri: {} is completely new", accessUri);
        newDigitalMedia.add(mediaEvent);
      } else {
        var currentDigitalMedia = currentDigitalMedias.get(accessUri);
        var relatedSpecimenDois = entityRelationshipService.findNewSpecimenRelationshipsForMedia(
            currentDigitalMedia, pidMap.get(accessUri));
        if (equalityService.mediaAreEqual(currentDigitalMedia, digitalMedia, relatedSpecimenDois)) {
          log.debug("Received digital media is equal to digital media: {}",
              currentDigitalMedia.id());
          equalDigitalMedia.add(currentDigitalMedia);
        } else {
          var eventWithUpdatedEr = equalityService.setExistingEventDatesMedia(
              currentDigitalMedia, mediaEvent);
          log.debug("Digital Media Object with id: {} has received an update",
              currentDigitalMedia.id());
          changedDigitalMedia.add(
              new UpdatedDigitalMediaTuple(currentDigitalMedia, eventWithUpdatedEr,
                  relatedSpecimenDois));
        }
      }
    }
    return new MediaPreprocessResult(equalDigitalMedia, changedDigitalMedia, newDigitalMedia);
  }

  private void republishSpecimenEvent(DigitalSpecimenEvent event) {
    try {
      publisherService.republishSpecimenEvent(event);
    } catch (JsonProcessingException e) {
      log.error("Fatal exception, unable to republish specimen message due to invalid json", e);
    }
  }

  private void republishMediaEvent(DigitalMediaEvent event) {
    try {
      publisherService.republishMediaEvent(event);
    } catch (JsonProcessingException e) {
      log.error("Fatal exception, unable to republish media message due to invalid json", e);
    }

  }

  private Map<String, DigitalSpecimenRecord> getCurrentSpecimen(Set<DigitalSpecimenEvent> events)
      throws DisscoRepositoryException {
    var eventMap = events.stream().collect(Collectors.toMap(
        event -> event.digitalSpecimenWrapper().physicalSpecimenID(),
        Function.identity()
    ));
    return repository.getDigitalSpecimens(
            events.stream().map(event -> event.digitalSpecimenWrapper().physicalSpecimenID()).toList())
        .stream()
        .map(dbRecord -> {
          var event = eventMap.get(dbRecord.digitalSpecimenWrapper().physicalSpecimenID());
          return new DigitalSpecimenRecord(
              dbRecord.id(),
              dbRecord.midsLevel(),
              dbRecord.version(),
              dbRecord.created(),
              dbRecord.digitalSpecimenWrapper(),
              event.masList(),
              event.forceMasSchedule(),
              event.isDataFromSourceSystem(),
              List.of());
        })
        .collect(
            toMap(
                specimenRecord -> specimenRecord.digitalSpecimenWrapper().physicalSpecimenID(),
                Function.identity()));
  }

  private Map<String, DigitalMediaRecord> getCurrentMedia(
      Set<DigitalMediaEvent> mediaEvents) {
    var mediaURIs = mediaEvents.stream()
        .map(mediaEvent -> mediaEvent.digitalMediaWrapper().attributes().getAcAccessURI())
        .collect(Collectors.toSet());
    var eventMap = mediaEvents.stream().collect(Collectors.toMap(
        event -> event.digitalMediaWrapper().attributes().getAcAccessURI(),
        Function.identity()
    ));
    if (!mediaURIs.isEmpty()) {
      return mediaRepository.getExistingDigitalMedia(mediaURIs)
          .stream().filter(Objects::nonNull)
          .map(dbRecord ->
          {
            var event = eventMap.get(dbRecord.accessURI());
            return new DigitalMediaRecord(
                dbRecord.id(),
                dbRecord.accessURI(),
                dbRecord.version(), dbRecord.created(), event.masList(), dbRecord.attributes(),
                dbRecord.originalAttributes(),
                event.forceMasSchedule());
          })
          .collect(toMap(
              DigitalMediaRecord::accessURI,
              Function.identity(),
              (uri1, uri2) -> {
                log.warn("Duplicate URIs found for digital media");
                return uri1;
              }
          ));
    }
    return Map.of();
  }

  public void handleMessagesMediaRelationshipTombstone(
      List<DigitalMediaRelationshipTombstoneEvent> events) {
    log.info("Processing {} digital media relationship tombstone events", events.size());
    var uniqueEvents = uniqueMediaRelationshipTombstoneEvents(events);
    var mediaDois = uniqueEvents.stream()
        .map(DigitalMediaRelationshipTombstoneEvent::mediaDoi)
        .collect(Collectors.toSet());
    var currentDigitalMediaRecords = mediaRepository.getExistingDigitalMediaByDoi(mediaDois)
        .stream()
        .collect(Collectors.toMap(DigitalMediaRecord::id, Function.identity()));
    var updatedDigitalMediaTuples = uniqueEvents.stream()
        .map(event -> {
          try {
            return createDigitalMediaEventWithoutER(event, currentDigitalMediaRecords);
          } catch (JsonProcessingException e) {
            log.error("Failed to process media tombstone event for media id {}",
                event.mediaDoi(), e);
            throw new JsonMappingException(e);
          }
        })
        .filter(Optional::isPresent).map(Optional::get).toList();
    if (updatedDigitalMediaTuples.isEmpty()) {
      log.info("No media relationships to tombstone");
      return;
    }
    log.info("Relationships removed for: {} digital media objects, processing updates",
        updatedDigitalMediaTuples.size());
    digitalMediaService.updateExistingDigitalMedia(
        updatedDigitalMediaTuples,
        false);
  }

  private List<DigitalMediaRelationshipTombstoneEvent> uniqueMediaRelationshipTombstoneEvents(
      List<DigitalMediaRelationshipTombstoneEvent> events) {
    var uniqueSet = new LinkedHashSet<DigitalMediaRelationshipTombstoneEvent>();
    var map = events.stream()
        .collect(Collectors.groupingBy(DigitalMediaRelationshipTombstoneEvent::mediaDoi));
    for (var entry : map.entrySet()) {
      if (entry.getValue().size() > 1) {
        log.warn("Found {} duplicate media relationship tombstone events in batch for media id {}",
            entry.getValue().size(), entry.getKey());
        for (int i = 0; i < entry.getValue().size(); i++) {
          if (i == 0) {
            uniqueSet.add(entry.getValue().get(i));
          } else {
            republishMediaRelationshipTombstoneEvent(entry.getValue().get(i));
          }
        }
      } else {
        uniqueSet.add(entry.getValue().getFirst());
      }
    }
    return new ArrayList<>(uniqueSet);
  }

  private void republishMediaRelationshipTombstoneEvent(
      DigitalMediaRelationshipTombstoneEvent event) {
    try {
      publisherService.publishDigitalMediaRelationTombstone(event);
    } catch (JsonProcessingException e) {
      log.error("Fatal exception, unable to republish specimen message due to invalid json", e);
    }
  }

  private Optional<UpdatedDigitalMediaTuple> createDigitalMediaEventWithoutER(
      DigitalMediaRelationshipTombstoneEvent event, Map<String, DigitalMediaRecord> existingMedia)
      throws JsonProcessingException {
    var currentDigitalMediaRecord = existingMedia.get(event.mediaDoi());
    var updatedDigitalMediaEvent = generatedUpdatedMediaEvent(event, currentDigitalMediaRecord);
    if (Objects.equals(currentDigitalMediaRecord.attributes(),
        updatedDigitalMediaEvent.digitalMediaWrapper().attributes())) {
      log.warn("No change in digital media: {} after removing relationship to specimen {}",
          event.mediaDoi(), event.specimenDoi());
      return Optional.empty();
    }
    return Optional.of(
        new UpdatedDigitalMediaTuple(currentDigitalMediaRecord, updatedDigitalMediaEvent,
            Collections.emptySet()));
  }

  private DigitalMediaEvent generatedUpdatedMediaEvent(DigitalMediaRelationshipTombstoneEvent event,
      DigitalMediaRecord currentDigitalMediaRecord) throws JsonProcessingException {
    var updatedDigitalMediaAttributes = deepCopy(currentDigitalMediaRecord.attributes());
    updatedDigitalMediaAttributes.setOdsHasEntityRelationships(
        removeRelationship(event, updatedDigitalMediaAttributes));
    return new DigitalMediaEvent(Collections.emptySet(),
        new DigitalMediaWrapper(updatedDigitalMediaAttributes.getOdsFdoType(),
            updatedDigitalMediaAttributes, objectMapper.createObjectNode()), false);
  }

  private DigitalMedia deepCopy(DigitalMedia currentDigitalMedia)
      throws JsonProcessingException {
    return objectMapper
        .readValue(objectMapper.writeValueAsString(currentDigitalMedia), DigitalMedia.class);
  }

}

