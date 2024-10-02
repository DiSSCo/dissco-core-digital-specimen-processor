package eu.dissco.core.digitalspecimenprocessor.service;

import static eu.dissco.core.digitalspecimenprocessor.domain.EntityRelationshipType.HAS_MEDIA;

import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaEventWithoutDOI;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaProcessResult;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.UpdatedDigitalSpecimenRecord;
import eu.dissco.core.digitalspecimenprocessor.repository.DigitalMediaRepository;
import eu.dissco.core.digitalspecimenprocessor.schema.EntityRelationship;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class DigitalMediaService {

  private final DigitalMediaRepository mediaRepository;

  public DigitalMediaProcessResult getExistingDigitalMedia(
      List<EntityRelationship> currentEntityRelationships,
      List<DigitalMediaEventWithoutDOI> digitalMediaEvents) {
    var currentMediaRelationships = currentEntityRelationships.stream()
        .filter(entityRelationship -> entityRelationship.getDwcRelationshipOfResource()
            .equals(HAS_MEDIA.getName()))
        .toList();
    var currentMediaIds = currentMediaRelationships.stream()
        .map(EntityRelationship::getDwcRelatedResourceID)
        .toList();
    var incomingMediaUris = digitalMediaEvents.stream()
        .map(media -> media.digitalMediaObjectWithoutDoi().attributes().getAcAccessURI())
        .toList();
    var existingMediaMap = mediaRepository.getDigitalMediaUrisFromId(currentMediaIds);
    var unchangedMedia = new ArrayList<EntityRelationship>();
    var tombstoneMedia = new ArrayList<EntityRelationship>();
    var newMedia = new ArrayList<DigitalMediaEventWithoutDOI>();
    existingMediaMap.forEach((mediaUri, mediaPid) -> {
      var entityRelationship = findErByMediaPid(currentMediaRelationships, mediaPid);
      if (incomingMediaUris.contains(mediaUri)) {
        unchangedMedia.add(entityRelationship);
      } else {
        tombstoneMedia.add(entityRelationship);
      }
    });
    digitalMediaEvents.stream().filter(event -> !existingMediaMap.containsKey(
            event.digitalMediaObjectWithoutDoi().attributes().getAcAccessURI()))
        .forEach(newMedia::add);

    log.info(
        "Identified {} unchanged media relationships, {} tombstoned media relationships, and {} new media",
        unchangedMedia.size(), tombstoneMedia.size(), newMedia.size());
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
