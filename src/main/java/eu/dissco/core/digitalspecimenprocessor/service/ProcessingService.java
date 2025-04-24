package eu.dissco.core.digitalspecimenprocessor.service;

import static java.util.stream.Collectors.toMap;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import eu.dissco.core.digitalspecimenprocessor.domain.SpecimenProcessResult;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaEvent;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaRecord;
import eu.dissco.core.digitalspecimenprocessor.domain.media.MediaProcessResult;
import eu.dissco.core.digitalspecimenprocessor.domain.media.UpdatedDigitalMediaTuple;
import eu.dissco.core.digitalspecimenprocessor.domain.relation.PidProcessResult;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenEvent;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenRecord;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.UpdatedDigitalSpecimenTuple;
import eu.dissco.core.digitalspecimenprocessor.exception.DisscoRepositoryException;
import eu.dissco.core.digitalspecimenprocessor.exception.PidException;
import eu.dissco.core.digitalspecimenprocessor.repository.DigitalMediaRepository;
import eu.dissco.core.digitalspecimenprocessor.repository.DigitalSpecimenRepository;
import eu.dissco.core.digitalspecimenprocessor.web.HandleComponent;
import java.util.ArrayList;
import java.util.Collection;
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
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class ProcessingService {

  private final DigitalSpecimenRepository repository;
  private final DigitalMediaRepository mediaRepository;
  private final DigitalSpecimenService digitalSpecimenService;
  private final DigitalMediaService digitalMediaService;
  private final EntityRelationshipService entityRelationshipService;
  private final EqualityService equalityService;
  private final RabbitMqPublisherService publisherService;
  private final FdoRecordService fdoRecordService;
  private final HandleComponent handleComponent;

  public List<DigitalSpecimenRecord> handleMessages(List<DigitalSpecimenEvent> events) {
    log.info("Processing {} digital specimen", events.size());
    try {
      var uniqueBatchSpecimens = removeDuplicateSpecimensInBatch(events);
      var uniqueBatchMedia = removeDuplicateMediaInBatch(events);
      var existingSpecimens = getCurrentSpecimen(uniqueBatchSpecimens);
      var existingMedia = getCurrentMedia(uniqueBatchMedia);
      var specimenProcessResult = processSpecimens(uniqueBatchSpecimens, existingSpecimens,
          existingMedia);
      var pids = processPids(specimenProcessResult, existingMedia, events, uniqueBatchMedia);
      var mediaProcessResult = processMedia(uniqueBatchMedia, existingMedia, pids.getRight());
      log.info("Batch consists of: {} new, {} update, and {} equal specimen",
          specimenProcessResult.newSpecimens().size(),
          specimenProcessResult.changedSpecimens().size(),
          specimenProcessResult.equalSpecimens().size());
      log.info("Batch consists of {} new, {} update, and {} equal media",
          mediaProcessResult.newDigitalMedia().size(),
          mediaProcessResult.changedDigitalMedia().size(),
          mediaProcessResult.equalDigitalMedia().size());
      var specimenResults = processSpecimenResults(specimenProcessResult, pids.getLeft());
      var mediaPids = updateMediaPidsWithResults(specimenResults, specimenProcessResult,
          pids.getRight());
      processMediaResults(mediaProcessResult, mediaPids);
      log.info("Processed specimen and media");
      return specimenResults;
    } catch (DisscoRepositoryException e) {
      log.error("Unable to access database", e);
      return List.of();
    }
  }

  private static Map<String, PidProcessResult> updateMediaPidsWithResults(
      List<DigitalSpecimenRecord> specimenResults, SpecimenProcessResult specimenProcessResult,
      Map<String, PidProcessResult> mediaPidsFull) {
    if (specimenResults.size() < (specimenProcessResult.changedSpecimens().size()
        + specimenProcessResult.newSpecimens().size())) {
      return mediaPidsFull;
    }
    // If we had a partial success, and not all specimens were created, we don't want to create meaningless ERS on our media
    // So we filter out the specimen PIDs that were not in our results
    var specimenDOIs = specimenResults.stream().map(DigitalSpecimenRecord::id).toList();
    var mediaPidsFiltered = new HashMap<String, PidProcessResult>();
    for (var mediaPid : mediaPidsFull.entrySet()) {
      var relatedDois = mediaPid.getValue().relatedDois().stream().filter(
          specimenDOIs::contains
      ).collect(Collectors.toSet());
      mediaPidsFiltered.put(mediaPid.getKey(),
          new PidProcessResult(mediaPid.getValue().doi(), relatedDois));
    }
    return mediaPidsFiltered;
  }

  /*
   * We need a way to map the specimen PIDs to the media and vice versa
   * Each has a many-to-many relationship
   * */
  private Pair<Map<String, PidProcessResult>, Map<String, PidProcessResult>> processPids(
      SpecimenProcessResult specimenProcessResult,
      Map<String, DigitalMediaRecord> existingMedias,
      List<DigitalSpecimenEvent> digitalSpecimenEvents,
      Set<DigitalMediaEvent> digitalMediaEvents) {
    var specimenPids = new HashMap<String, PidProcessResult>();
    var mediaHashMap = new HashMap<String, HashSet<String>>(); // key = local id
    var allSpecimenPids = concatSpecimenPids(specimenProcessResult);
    var newMediaPids = createPidsForNewMediaObjects(existingMedias, digitalMediaEvents);
    var allMediaPids = concatMediaPids(existingMedias, newMediaPids);
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
    return Pair.of(specimenPids, mediaPids);
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
      SpecimenProcessResult specimenProcessResult) {
    var existingSpecimenPids = Stream.concat(
        specimenProcessResult.equalSpecimens().stream(),
        specimenProcessResult.changedSpecimens().stream()
            .map(UpdatedDigitalSpecimenTuple::currentSpecimen)
    ).collect(toMap(
        specimen -> specimen.digitalSpecimenWrapper().physicalSpecimenID(),
        DigitalSpecimenRecord::id
    ));
    return concatMaps(specimenProcessResult.newSpecimenPids(), existingSpecimenPids);
  }

  private static Map<String, String> concatMediaPids(
      Map<String, DigitalMediaRecord> existingMedias, Map<String, String> newMediaPids) {
    var existingPidMap =  existingMedias.entrySet()
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

  private List<DigitalSpecimenRecord> processSpecimenResults(
      SpecimenProcessResult specimenProcessResult,
      Map<String, PidProcessResult> pidProcessResults) {
    var results = new ArrayList<DigitalSpecimenRecord>();
    if (!specimenProcessResult.equalSpecimens().isEmpty()) {
      digitalSpecimenService.updateEqualSpecimen(specimenProcessResult.equalSpecimens());
    }
    if (!specimenProcessResult.newSpecimens().isEmpty() && !specimenProcessResult.newSpecimenPids()
        .isEmpty()) {
      results.addAll(
          digitalSpecimenService.createNewDigitalSpecimen(specimenProcessResult.newSpecimens(),
              pidProcessResults));
    }
    if (!specimenProcessResult.changedSpecimens().isEmpty()) {
      results.addAll(digitalSpecimenService.updateExistingDigitalSpecimen(
          specimenProcessResult.changedSpecimens(), pidProcessResults));
    }
    return results;
  }

  private List<DigitalMediaRecord> processMediaResults(MediaProcessResult mediaProcessResult,
      Map<String, PidProcessResult> pidProcessResults) {
    var results = new ArrayList<DigitalMediaRecord>();
    if (!mediaProcessResult.equalDigitalMedia().isEmpty()) {
      digitalMediaService.processEqualDigitalMedia(mediaProcessResult.equalDigitalMedia());
    }
    if (!mediaProcessResult.newDigitalMedia().isEmpty()) {
      results.addAll(
          digitalMediaService.createNewDigitalMedia(mediaProcessResult.newDigitalMedia(),
              pidProcessResults));
    }
    if (!mediaProcessResult.changedDigitalMedia().isEmpty()) {
      results.addAll(
          digitalMediaService.updateExistingDigitalMedia(mediaProcessResult.changedDigitalMedia(),
              pidProcessResults));
    }
    return results;
  }


  private Set<DigitalSpecimenEvent> removeDuplicateSpecimensInBatch(
      List<DigitalSpecimenEvent> events) {
    var uniqueSet = new LinkedHashSet<DigitalSpecimenEvent>();
    var map = events.stream()
        .collect(
            Collectors.groupingBy(event -> event.digitalSpecimenWrapper().physicalSpecimenID()));
    for (Entry<String, List<DigitalSpecimenEvent>> entry : map.entrySet()) {
      if (entry.getValue().size() > 1) {
        log.warn("Found {} duplicate media in batch for id {}", entry.getValue().size(),
            entry.getKey());
        for (int i = 0; i < entry.getValue().size(); i++) {
          if (i == 0) {
            uniqueSet.add(entry.getValue().get(i));
          } else {
            republishSpecimenEvent(entry.getValue().get(i));
          }
        }
      } else {
        uniqueSet.add(entry.getValue().get(0));
      }
    }
    return uniqueSet;
  }

  private Set<DigitalMediaEvent> removeDuplicateMediaInBatch(
      List<DigitalSpecimenEvent> specimenEvents) {
    var mediaEvents = specimenEvents.stream()
        .map(DigitalSpecimenEvent::digitalMediaEvents)
        .flatMap(Collection::stream)
        .toList();
    var uniqueSet = new LinkedHashSet<DigitalMediaEvent>();
    var map = mediaEvents.stream()
        .collect(
            Collectors.groupingBy(
                event -> event.digitalMediaWrapper().attributes().getAcAccessURI()));
    for (var entry : map.entrySet()) {
      if (entry.getValue().size() > 1) {
        log.warn("Found {} duplicate specimens in batch for id {}", entry.getValue().size(),
            entry.getKey());
        for (int i = 0; i < entry.getValue().size(); i++) {
          if (i == 0) {
            uniqueSet.add(entry.getValue().get(i));
          } else {
            republishMediaEvent(entry.getValue().get(i));
          }
        }
      } else {
        uniqueSet.add(entry.getValue().get(0));
      }
    }
    return uniqueSet;
  }

  private SpecimenProcessResult processSpecimens(Set<DigitalSpecimenEvent> events,
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
        var processedMediaRelationships = entityRelationshipService.processMediaRelationshipsForSpecimen(
            currentSpecimens, event, currentMedia);
        if (equalityService.specimensAreEqual(currentDigitalSpecimen.digitalSpecimenWrapper(),
            digitalSpecimenWrapper, processedMediaRelationships)) {
          log.debug("Received digital specimen is equal to digital specimen: {}",
              currentDigitalSpecimen.id());
          equalSpecimens.add(currentDigitalSpecimen);
        } else {
          log.debug("Specimen with id: {} has received an update", currentDigitalSpecimen.id());
          var eventWithUpdatedEr = equalityService.setEventDatesSpecimen(
              currentDigitalSpecimen.digitalSpecimenWrapper(), event);
          changedSpecimens.add(
              new UpdatedDigitalSpecimenTuple(currentDigitalSpecimen, eventWithUpdatedEr,
                  processedMediaRelationships));
        }
      }
    }
    var newSpecimenPids = createNewSpecimenPids(newSpecimens);
    return new SpecimenProcessResult(equalSpecimens, changedSpecimens, newSpecimens,
        newSpecimenPids);
  }

  private Map<String, String> createNewSpecimenPids(List<DigitalSpecimenEvent> events) {
    if (events.isEmpty()) {
      return Map.of();
    }
    var specimenList = events.stream().map(DigitalSpecimenEvent::digitalSpecimenWrapper).toList();
    var request = fdoRecordService.buildPostHandleRequest(specimenList);
    return createNewPids(request, true);
  }

  private Map<String, String> createPidsForNewMediaObjects(
      Map<String, DigitalMediaRecord> existingMedia,
      Set<DigitalMediaEvent> digitalMediaEvents) {
    var newEvents = digitalMediaEvents.stream()
        .filter(e -> !existingMedia.containsKey(e.digitalMediaWrapper().accessUri())).toList();
    return createNewMediaPids(newEvents);
  }

  private Map<String, String> createNewMediaPids(
      List<DigitalMediaEvent> digitalMediaEvents) {
    if (digitalMediaEvents.isEmpty()) {
      return Map.of();
    }
    var request = fdoRecordService.buildPostRequestMedia(digitalMediaEvents);
    return createNewPids(request, false);
  }

  // todo improve error handling
  private Map<String, String> createNewPids(List<JsonNode> request, boolean isSpecimen) {
    try {
      return handleComponent.postHandle(request, isSpecimen);
    } catch (PidException e) {
      log.error("Unable to create new PIDs", e);
    }
    return Map.of();
  }


  private MediaProcessResult processMedia(Set<DigitalMediaEvent> events,
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
          var eventWithUpdatedEr = equalityService.setEventDatesMedia(
              currentDigitalMedia, mediaEvent);
          log.debug("Digital Media Object with id: {} has received an update",
              currentDigitalMedia.id());
          changedDigitalMedia.add(
              new UpdatedDigitalMediaTuple(currentDigitalMedia, eventWithUpdatedEr,
                  relatedSpecimenDois));
        }
      }
    }
    return new MediaProcessResult(equalDigitalMedia, changedDigitalMedia, newDigitalMedia
    );
  }

  private void republishSpecimenEvent(DigitalSpecimenEvent event) {
    try {
      publisherService.republishSpecimenEvent(event);
    } catch (JsonProcessingException e) {
      log.error("Fatal exception, unable to republish message due to invalid json", e);
    }
  }

  private void republishMediaEvent(DigitalMediaEvent event) {
    try {
      publisherService.republishMediaEvent(event);
    } catch (JsonProcessingException e) {
      log.error("Fatal exception, unable to republish message due to invalid json", e);
    }
  }

  private Map<String, DigitalSpecimenRecord> getCurrentSpecimen(Set<DigitalSpecimenEvent> events)
      throws DisscoRepositoryException {
    return repository.getDigitalSpecimens(
            events.stream().map(event -> event.digitalSpecimenWrapper().physicalSpecimenID()).toList())
        .stream().collect(
            toMap(
                specimenRecord -> specimenRecord.digitalSpecimenWrapper().physicalSpecimenID(),
                Function.identity()));
  }

  private Map<String, DigitalMediaRecord> getCurrentMedia(
      Set<DigitalMediaEvent> mediaEvents) {
    var mediaURIs = mediaEvents.stream()
        .map(mediaEvent -> mediaEvent.digitalMediaWrapper().attributes().getAcAccessURI())
        .collect(Collectors.toSet());
    return mediaRepository.getExistingDigitalMedia(mediaURIs)
        .stream().filter(Objects::nonNull)
        .collect(toMap(
            DigitalMediaRecord::accessURI,
            Function.identity(),
            (uri1, uri2) -> {
              log.warn("Duplicate URIs found for digital media");
              return uri1;
            }
        ));
  }

}

