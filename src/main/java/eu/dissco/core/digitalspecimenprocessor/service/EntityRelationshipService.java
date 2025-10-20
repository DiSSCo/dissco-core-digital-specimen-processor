package eu.dissco.core.digitalspecimenprocessor.service;

import static eu.dissco.core.digitalspecimenprocessor.domain.EntityRelationshipType.HAS_MEDIA;
import static eu.dissco.core.digitalspecimenprocessor.util.DigitalObjectUtils.DOI_PROXY;

import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaEvent;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaRecord;
import eu.dissco.core.digitalspecimenprocessor.domain.relation.MediaRelationshipProcessResult;
import eu.dissco.core.digitalspecimenprocessor.domain.relation.PidProcessResult;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenEvent;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenRecord;
import eu.dissco.core.digitalspecimenprocessor.schema.EntityRelationship;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class EntityRelationshipService {

  // Returns map of Specimen DOI to its media process result
  public MediaRelationshipProcessResult processMediaRelationshipsForSpecimen(
      Map<String, DigitalSpecimenRecord> currentSpecimens,
      DigitalSpecimenEvent digitalSpecimenEvent,
      Map<String, DigitalMediaRecord> currentMedia
  ) {
    if (currentMedia.isEmpty() && digitalSpecimenEvent.digitalMediaEvents().isEmpty()) {
      return new MediaRelationshipProcessResult(List.of(), List.of(), List.of());
    }
    // Media Uri -> DOI
    var mediaIdMap = getExistingMediaIdMap(currentMedia);
    var currentSpecimen = currentSpecimens.get(
        digitalSpecimenEvent.digitalSpecimenWrapper().physicalSpecimenID());
    var currentMediaErs = getMediaEntityRelationshipsForSpecimen(currentSpecimen);
    var newMediaErs = getNewLinkedMedia(digitalSpecimenEvent, mediaIdMap, currentMediaErs);
    var tombstonedMediaErs = getTombstonedMediaRelationships(digitalSpecimenEvent, currentMediaErs,
        mediaIdMap);
    var unchangedMediaErs = getUnchangedMediaEntityRelationships(tombstonedMediaErs,
        currentMediaErs);
    return new MediaRelationshipProcessResult(tombstonedMediaErs, newMediaErs, unchangedMediaErs);
  }

  public Set<String> findNewSpecimenRelationshipsForMedia(
      DigitalMediaRecord digitalMediaRecord, PidProcessResult pidProcessResult) {
    if (digitalMediaRecord.attributes().getOdsHasEntityRelationships() == null) {
      return Set.of();
    }
    var existingLinkedPids = digitalMediaRecord.attributes().getOdsHasEntityRelationships()
        .stream()
        .map(EntityRelationship::getDwcRelatedResourceID)
        .filter(Objects::nonNull)
        .map(id -> id.replace(DOI_PROXY, ""))
        .collect(Collectors.toSet());
    return pidProcessResult.doisOfRelatedObjects().stream()
        .filter(Objects::nonNull)
        .filter(relatedDoi -> !existingLinkedPids.contains(relatedDoi)).collect(Collectors.toSet());
  }

  /*
 In our (incoming) mediaEvents, we are missing mediaID but we have the url
 In the existing entity relationships in the specimen, we are missing the media url but we have the id
 Returns Map<Media DOI, URI>
 */
  private Map<String, String> getExistingMediaIdMap(Map<String, DigitalMediaRecord> currentMedia) {
    return currentMedia.entrySet().stream()
        .collect(Collectors.toMap(
            entry -> entry.getValue().id(),
            Entry::getKey
        ));
  }

  private List<DigitalMediaEvent> getNewLinkedMedia(
      DigitalSpecimenEvent digitalSpecimenEvent, Map<String, String> mediaIdMap,
      List<EntityRelationship> currentMediaErs) {
    var currentMediaUris = currentMediaErs
        .stream()
        .map(er -> {
          if (er.getDwcRelatedResourceID() == null){
            return null; // We will tombstone this ER if the id is null
          }
          return mediaIdMap.get(er.getDwcRelatedResourceID().replace(DOI_PROXY, ""));
        })
        .filter(Objects::nonNull)
        .collect(Collectors.toSet());
    return digitalSpecimenEvent.digitalMediaEvents()
        .stream()
        .filter(event -> !currentMediaUris.contains(
            event.digitalMediaWrapper().attributes().getAcAccessURI()))
        .toList();
  }

  // Identify which media ERs in the previous version are no longer relevant
  private List<EntityRelationship> getTombstonedMediaRelationships(
      DigitalSpecimenEvent digitalSpecimenEvent, List<EntityRelationship> currentMediaErs,
      Map<String, String> mediaIdMap) {
    // Get media URIs from this batch
    var incomingMediaUris = digitalSpecimenEvent.digitalMediaEvents().stream()
        .map(event -> event.digitalMediaWrapper().attributes().getAcAccessURI()).toList();
    // If a media ER is NOT associated with a URI that is in this current batch, that means the relationship no longer exists
    return currentMediaErs.stream().filter(
            er -> {
              // Error recovery; if there's a null id we want it in our tombstoned list
              // So that it marks the specimen as needing to be updated
              if (er.getDwcRelatedResourceID() == null) {
                return true;
              }
              var mediaUri = mediaIdMap.get(
                  er.getDwcRelatedResourceID().replace(DOI_PROXY, ""));
              return !incomingMediaUris.contains(mediaUri);
            }
        )
        .toList();
  }

  private List<EntityRelationship> getUnchangedMediaEntityRelationships(
      List<EntityRelationship> tombstonedMediaRelationships,
      List<EntityRelationship> currentMediaErs) {
    var tombstonedSet = new HashSet<>(tombstonedMediaRelationships);
    return currentMediaErs.stream().filter(
        er -> !tombstonedSet.contains(er)
    ).toList();
  }

  private static List<EntityRelationship> getMediaEntityRelationshipsForSpecimen(
      DigitalSpecimenRecord digitalSpecimenRecord) {
    return digitalSpecimenRecord.digitalSpecimenWrapper().attributes()
        .getOdsHasEntityRelationships()
        .stream()
        .filter(entityRelationship -> entityRelationship.getDwcRelationshipOfResource()
            .equals(HAS_MEDIA.getRelationshipName()))
        .toList();
  }
}
