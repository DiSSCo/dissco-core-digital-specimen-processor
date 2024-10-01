package eu.dissco.core.digitalspecimenprocessor.service;

import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaEventWithoutDOI;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaProcessResult;
import eu.dissco.core.digitalspecimenprocessor.repository.DigitalMediaRepository;
import eu.dissco.core.digitalspecimenprocessor.schema.EntityRelationship;
import java.util.ArrayList;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class DigitalMediaService {

  private final DigitalMediaRepository mediaRepository;

  public DigitalMediaProcessResult getExistingDigitalMedia(
      List<EntityRelationship> currentEntityRelationships,
      List<DigitalMediaEventWithoutDOI> digitalMediaEvents) {
    var currentMediaUris = currentEntityRelationships.stream()
        .map(EntityRelationship::getDwcRelatedResourceID)
        .toList();
    var incomingMediaUris = digitalMediaEvents.stream()
        .map(media -> media.digitalMediaObjectWithoutDoi().attributes().getAcAccessURI())
        .toList();
    var existingMediaMap = mediaRepository.getDigitalMediaDois(currentMediaUris);
    var unchangedMedia = new ArrayList<EntityRelationship>();
    var tombstoneMedia = new ArrayList<EntityRelationship>();
    var newMedia = new ArrayList<DigitalMediaEventWithoutDOI>();
    for (var foundMedia : existingMediaMap.entrySet()) {
      var mediaUri = foundMedia.getKey();
      if (currentMediaUris.contains(mediaUri)) {
        if (incomingMediaUris.contains(mediaUri)) {
          unchangedMedia.add(
              findErByMediaPid(currentEntityRelationships, existingMediaMap.get(mediaUri)));
        } else {
          tombstoneMedia.add(
              findErByMediaPid(currentEntityRelationships, existingMediaMap.get(mediaUri)));
        }
      } else {
        newMedia.add(findDigitalMediaEventByUri(digitalMediaEvents, mediaUri));
      }
    }
    return new DigitalMediaProcessResult(unchangedMedia, tombstoneMedia, newMedia);
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

  private static DigitalMediaEventWithoutDOI findDigitalMediaEventByUri(
      List<DigitalMediaEventWithoutDOI> digitalMediaEvents, String mediaUri) {
    for (var digitalMediaEvent : digitalMediaEvents) {
      if (digitalMediaEvent.digitalMediaObjectWithoutDoi().attributes().getAcAccessURI()
          .equals(mediaUri)) {
        return digitalMediaEvent;
      }
    }
    throw new IllegalStateException();
  }


}
