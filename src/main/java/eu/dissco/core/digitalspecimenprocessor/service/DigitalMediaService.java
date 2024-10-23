package eu.dissco.core.digitalspecimenprocessor.service;

import static eu.dissco.core.digitalspecimenprocessor.domain.EntityRelationshipType.HAS_MEDIA;

import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaEventWithoutDOI;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaKey;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaProcessResult;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenRecord;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.UpdatedDigitalSpecimenRecord;
import eu.dissco.core.digitalspecimenprocessor.repository.DigitalMediaRepository;
import eu.dissco.core.digitalspecimenprocessor.schema.EntityRelationship;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class DigitalMediaService {

  private final DigitalMediaRepository mediaRepository;

  public Map<String, DigitalMediaProcessResult> getExistingDigitalMedia(
      Map<String, DigitalSpecimenRecord> currentSpecimens,
      List<DigitalMediaEventWithoutDOI> mediaEvents) {
    var mediaProcessResults = new HashMap<String, DigitalMediaProcessResult>();
    var mediaMap = new HashMap<String, List<DigitalMediaEventWithoutDOI>>();
    mediaEvents.forEach(mediaEvent ->
        mediaMap.computeIfAbsent(mediaEvent.digitalMediaObjectWithoutDoi().physicalSpecimenID(),
            k -> new ArrayList<>()).add(mediaEvent)
    );
    var mediaIds = getAllMediaIds(currentSpecimens.values());
    for (var entry : currentSpecimens.entrySet()) {
      var currentSpecimen = entry.getValue();
      // use physical specimen id to get relevant media events
      var mediaEventsForSpecimen =
          mediaMap.get(entry.getKey()) == null ?
              new ArrayList<DigitalMediaEventWithoutDOI>() : mediaMap.get(entry.getKey());
      var existingMediaForCurrentSpecimen = mediaIds.entrySet().stream()
          .filter(mediaId -> mediaId.getKey().digitalSpecimenId().equals(currentSpecimen.id()))
          .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
      var processResult = getExistingDigitalMediaProcessResult(
          currentSpecimen.digitalSpecimenWrapper().attributes().getOdsHasEntityRelationship(),
          mediaEventsForSpecimen, existingMediaForCurrentSpecimen, currentSpecimen.id());
      mediaProcessResults.put(currentSpecimen.id(), processResult);
    }
    return mediaProcessResults;
  }

  /*
   In our (incoming) mediaEvents, we are missing mediaID but we have the url
   In the existing entity relationships in the specimen, we are missing the media url but we have the id
   This function gets the mediaUrls based on the media id from the ER
   That way we can match incoming media events to existing media objects and find which media are new
   */
  private Map<DigitalMediaKey, String> getAllMediaIds(
      Collection<DigitalSpecimenRecord> currentSpecimens) {
    var currentMediaIds = currentSpecimens.stream()
        .map(digitalSpecimenRecord -> digitalSpecimenRecord.digitalSpecimenWrapper().attributes()
            .getOdsHasEntityRelationship())
        .flatMap(List::stream)
        .filter(entityRelationship -> entityRelationship.getDwcRelationshipOfResource()
            .equals(HAS_MEDIA.getName()))
        .map(EntityRelationship::getDwcRelatedResourceID)
        .toList();
    return mediaRepository.getDigitalMediaUrisFromId(currentMediaIds);
  }


  private DigitalMediaProcessResult getExistingDigitalMediaProcessResult(
      List<EntityRelationship> currentEntityRelationships,
      List<DigitalMediaEventWithoutDOI> digitalMediaEvents,
      Map<DigitalMediaKey, String> existingMediaMap, String digitalSpecimenId) {
    var currentMediaRelationships = currentEntityRelationships.stream()
        .filter(entityRelationship -> entityRelationship.getDwcRelationshipOfResource()
            .equals(HAS_MEDIA.getName()))
        .toList();
    var incomingMediaUris = digitalMediaEvents.stream()
        .map(media -> media.digitalMediaObjectWithoutDoi().attributes().getAcAccessURI())
        .toList();
    var unchangedMedia = new ArrayList<EntityRelationship>();
    var tombstoneMedia = new ArrayList<EntityRelationship>();
    var newMedia = new ArrayList<DigitalMediaEventWithoutDOI>();
    existingMediaMap.forEach((mediaKey, mediaPid) -> {
      var entityRelationship = findErByMediaPid(currentMediaRelationships, mediaPid);
      if (incomingMediaUris.contains(mediaKey.mediaUrl())) {
        unchangedMedia.add(entityRelationship);
      } else {
        tombstoneMedia.add(entityRelationship);
      }
    });
    digitalMediaEvents.stream().filter(event -> !existingMediaMap.containsKey(
            new DigitalMediaKey(digitalSpecimenId,
                event.digitalMediaObjectWithoutDoi().attributes().getAcAccessURI())))
        .forEach(newMedia::add);
    if (!unchangedMedia.isEmpty() || !tombstoneMedia.isEmpty() || !newMedia.isEmpty()) {
      log.info(
          "Identified {} unchanged media relationships, {} tombstoned media relationships, and {} new media for specimen {}",
          unchangedMedia.size(), tombstoneMedia.size(), newMedia.size(), digitalSpecimenId);
    } else {
      log.debug("No media relationships associated with specimen {}", digitalSpecimenId);
    }
    return new DigitalMediaProcessResult(unchangedMedia, tombstoneMedia, newMedia);
  }

  public void removeSpecimenRelationshipsFromMedia(
      Set<UpdatedDigitalSpecimenRecord> updatedDigitalSpecimenRecords) {
    var mediaIds = updatedDigitalSpecimenRecords.stream()
        .map(specimenRecord -> specimenRecord.digitalMediaProcessResult().tombstoneMedia())
        .flatMap(List::stream)
        .map(EntityRelationship::getDwcRelatedResourceID)
        .toList();
    if (!mediaIds.isEmpty()) {
      log.info("Removing {} tombstoned media relationships from database", mediaIds.size());
      mediaRepository.removeSpecimenRelationshipsFromMedia(mediaIds);
    }
  }

  private static EntityRelationship findErByMediaPid(List<EntityRelationship> entityRelationships,
      String mediaPid) {
    for (var entityRelationship : entityRelationships) {
      if (entityRelationship.getDwcRelatedResourceID().equals(mediaPid)) {
        return entityRelationship;
      }
    }
    throw new IllegalStateException();
  }

}
