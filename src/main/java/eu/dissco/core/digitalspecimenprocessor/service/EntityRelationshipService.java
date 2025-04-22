package eu.dissco.core.digitalspecimenprocessor.service;

import static eu.dissco.core.digitalspecimenprocessor.domain.EntityRelationshipType.HAS_MEDIA;
import static eu.dissco.core.digitalspecimenprocessor.util.DigitalObjectUtils.DOI_PREFIX;

import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaEventWithoutDOI;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaRecord;
import eu.dissco.core.digitalspecimenprocessor.domain.relation.MediaRelationshipProcessResult;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenEvent;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenRecord;
import eu.dissco.core.digitalspecimenprocessor.schema.EntityRelationship;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class EntityRelationshipService {

  // Returns map of Specimen DOI to its media process rsult
  public MediaRelationshipProcessResult processMediaRelationshipsForSpecimen(
      Map<String, DigitalSpecimenRecord> currentSpecimens,
      DigitalSpecimenEvent digitalSpecimenEvent,
      Map<String, DigitalMediaRecord> currentMedia
  ) {
    if (currentMedia.isEmpty()){
      return new MediaRelationshipProcessResult(List.of(), List.of());
    }
    // Media Uri -> DOI
    var mediaIdMap = getExistingMediaIDMap(currentMedia);
    var currentSpecimen = currentSpecimens.get(
        digitalSpecimenEvent.digitalSpecimenWrapper().physicalSpecimenID());
    var newMediaErs = getNewLinkedMedia(digitalSpecimenEvent, mediaIdMap);
    var tombstonedMedia = getTombstonedMediaRelationships(digitalSpecimenEvent, currentSpecimen,
        mediaIdMap);
    return new MediaRelationshipProcessResult(tombstonedMedia, newMediaErs);
  }

  /*
 In our (incoming) mediaEvents, we are missing mediaID but we have the url
 In the existing entity relationships in the specimen, we are missing the media url but we have the id
 Returns Map<URI, Media DOI>
 */
  private Map<String, String> getExistingMediaIDMap(Map<String, DigitalMediaRecord> currentMedia) {
    return currentMedia.entrySet().stream()
        .collect(Collectors.toMap(
            Entry::getKey,
            entry -> entry.getValue().id()
        ));
  }

  private List<DigitalMediaEventWithoutDOI> getNewLinkedMedia(
      DigitalSpecimenEvent digitalSpecimenEvent, Map<String, String> mediaIdMap) {
    return digitalSpecimenEvent.digitalMediaEvents().stream()
        .filter(mediaEvent -> mediaIdMap.containsKey(
            mediaEvent.digitalMediaObjectWithoutDoi().attributes().getAcAccessURI()))
        .toList();
  }

  // Identify which media ERs in the previous version are no longer relevant
  private List<EntityRelationship> getTombstonedMediaRelationships(
      DigitalSpecimenEvent digitalSpecimenEvent, DigitalSpecimenRecord currentSpecimen,
      Map<String, String> mediaIDMap) {
    // Invert media map so that its key is the DOI and the value is the URI
    var inverseMediaIDMap = mediaIDMap.entrySet().stream()
        .collect(Collectors.toMap(Entry::getValue, Entry::getKey));
    // Get media URIs from this batch
    var incomingMediaUris = digitalSpecimenEvent.digitalMediaEvents().stream()
        .map(event -> event.digitalMediaObjectWithoutDoi().attributes().getAcAccessURI()).toList();
    var currentErs = getMediaEntityRelationshipsForSpecimen(currentSpecimen);
    // If a media ER is NOT associated with a URI that is in this current batch, that means the relationship no longer exists
    return currentErs.stream().filter(
            er -> {
              var mediaUri = inverseMediaIDMap.get(
                  er.getDwcRelatedResourceID().replace(DOI_PREFIX, ""));
              return !incomingMediaUris.contains(mediaUri);
            }
        )
        .toList();
  }

  private static List<EntityRelationship> getMediaEntityRelationshipsForSpecimen(
      DigitalSpecimenRecord digitalSpecimenRecord) {
    return digitalSpecimenRecord.digitalSpecimenWrapper().attributes()
        .getOdsHasEntityRelationships()
        .stream()
        .filter(entityRelationship -> entityRelationship.getDwcRelationshipOfResource()
            .equals(HAS_MEDIA.getName()))
        .toList();
  }
}
