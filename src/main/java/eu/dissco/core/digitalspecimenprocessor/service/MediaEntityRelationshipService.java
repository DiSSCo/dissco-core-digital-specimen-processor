package eu.dissco.core.digitalspecimenprocessor.service;

import com.nimbusds.jose.util.Pair;
import eu.dissco.core.digitalspecimenprocessor.domain.DigitalMediaEventWithoutDOI;
import eu.dissco.core.digitalspecimenprocessor.domain.DigitalMediaUpdatePidEvent;
import eu.dissco.core.digitalspecimenprocessor.domain.DigitalMediaWithoutDOI;
import eu.dissco.core.digitalspecimenprocessor.domain.DigitalSpecimenEvent;
import eu.dissco.core.digitalspecimenprocessor.domain.DigitalSpecimenRecord;
import eu.dissco.core.digitalspecimenprocessor.domain.DigitalSpecimenWrapper;
import eu.dissco.core.digitalspecimenprocessor.domain.UpdatedDigitalSpecimenTuple;
import eu.dissco.core.digitalspecimenprocessor.property.FdoProperties;
import eu.dissco.core.digitalspecimenprocessor.schema.Agent;
import eu.dissco.core.digitalspecimenprocessor.schema.EntityRelationship;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class MediaEntityRelationshipService {

  private final FdoProperties fdoProperties;
  private static final String MEDIA_RELATIONSHIP = "hasMedia";

  public void addNewMediaERs(
      Map<DigitalSpecimenRecord, Pair<List<String>, List<DigitalMediaEventWithoutDOI>>> digitalSpecimenRecords) {
    digitalSpecimenRecords.forEach((key, value) -> addMediaERsToSpecimen(key, value.getRight()));
  }

  public List<UpdatedDigitalSpecimenTuple> updateMediaERs(
      List<DigitalSpecimenRecord> currentDigitalSpecimenRecords,
      List<DigitalMediaUpdatePidEvent> updateMediaEvents) {
    var updatedSpecimens = new ArrayList<UpdatedDigitalSpecimenTuple>();
    for (var digitalSpecimenRecord : currentDigitalSpecimenRecords) {
      var currentSpecimen = deepCopyDigitalSpecimenRecord(digitalSpecimenRecord);
      var updatedErList = updateMediaEr(digitalSpecimenRecord, updateMediaEvents);
      digitalSpecimenRecord.digitalSpecimenWrapper().attributes()
          .setOdsHasEntityRelationship(updatedErList);
      updatedSpecimens.add(new UpdatedDigitalSpecimenTuple(currentSpecimen,
          new DigitalSpecimenEvent(
              Collections.emptyList(),
              digitalSpecimenRecord.digitalSpecimenWrapper(),
              Collections.emptyList()
          )));
    }
    return updatedSpecimens;
  }

  /*
  The translator will not add the hasMedia ER to the incoming message, so we need to add them here. There are 2 kinds of media ERs to add:
    1 - Existing media: If the media has already been ingested, it will have a PID, which will not be present in the update event.
        Therefore, we take the ER from the previous version
    2 - New Media: if a media event is present in the incoming event that doesn't have a corresponding er in the previous version of the specimen,
        it must be a new object. In that case, we create an interim media er (i.e. with the URI as the related resource id)
  */
  public DigitalSpecimenWrapper getMediaErsForUpdatedSpecimen(DigitalSpecimenRecord currentSpecimen,
      DigitalSpecimenEvent digitalSpecimenEvent) {
    var currentMediaErs = currentSpecimen.digitalSpecimenWrapper()
        .attributes().getOdsHasEntityRelationship();
    var existingMediaUris = currentMediaErs.stream().map(
        EntityRelationship::getOdsRelatedResourceURI).toList();
    var existingMediaErs = currentMediaErs.stream()
        .filter(this::isDisscoMediaEr)
        .filter(er -> existingMediaUris.contains(er.getOdsRelatedResourceURI()))
        .toList();
    var newMediaErs = digitalSpecimenEvent.digitalMediaEvents()
        .stream()
        .filter(mediaEvent -> !existingMediaUris.contains(
            getUri(mediaEvent.digitalMediaObjectWithoutDoi())))
        .map(this::createInterimMediaEr)
        .toList();
    var eventErs = digitalSpecimenEvent.digitalSpecimenWrapper()
        .attributes().getOdsHasEntityRelationship();
    var allErs = Stream.concat(Stream.concat(existingMediaErs.stream(), newMediaErs.stream()),
        eventErs.stream()).toList();
    digitalSpecimenEvent.digitalSpecimenWrapper().attributes()
        .setOdsHasEntityRelationship(allErs);
    return digitalSpecimenEvent.digitalSpecimenWrapper();
  }

  private boolean isDisscoMediaEr(EntityRelationship er) {
    return er.getDwcRelationshipOfResource().equals(MEDIA_RELATIONSHIP) &&
        er.getDwcRelationshipAccordingTo().equals(fdoProperties.getApplicationName()) &&
        er.getOdsRelationshipAccordingToAgent().equals(buildDiSSCoAgent());
  }

  private DigitalSpecimenRecord deepCopyDigitalSpecimenRecord(
      DigitalSpecimenRecord digitalSpecimenRecord) {
    return new DigitalSpecimenRecord(
        digitalSpecimenRecord.id(),
        digitalSpecimenRecord.midsLevel(),
        digitalSpecimenRecord.version(),
        digitalSpecimenRecord.created(),
        digitalSpecimenRecord.digitalSpecimenWrapper()
    );
  }

  private List<EntityRelationship> updateMediaEr(DigitalSpecimenRecord digitalSpecimenRecord,
      List<DigitalMediaUpdatePidEvent> updateMediaEvents) {
    var specimenErList = new ArrayList<>(
        digitalSpecimenRecord.digitalSpecimenWrapper().attributes().getOdsHasEntityRelationship());
    for (var updateMediaEvent : updateMediaEvents) {
      var targetEr = getErFromURI(digitalSpecimenRecord, updateMediaEvent);
      if (targetEr.isPresent()) {
        specimenErList.remove(targetEr.get());
        targetEr.get()
            .withDwcRelationshipEstablishedDate(java.util.Date.from(
                Instant.now())) // todo should this update when we make the new ER?
            .withDwcRelatedResourceID(updateMediaEvent.digitalMediaPID())
            .withDwcRelationshipRemarks(null);
        specimenErList.add(targetEr.get());
      } else {
        log.warn(
            "Unable to identify single, unique media EntityRelationship in specimen with uri {}",
            updateMediaEvent.digitalMediaAccessURI());
      }
    }
    return specimenErList;
  }

  private void addMediaERsToSpecimen(DigitalSpecimenRecord digitalSpecimen,
      List<DigitalMediaEventWithoutDOI> digitalMediaList) {
    if (digitalMediaList.isEmpty()) {
      return;
    }
    var erList = new ArrayList<>(
        digitalSpecimen.digitalSpecimenWrapper().attributes().getOdsHasEntityRelationship());
    erList.addAll(digitalMediaList.stream().map(this::createInterimMediaEr).toList());
    digitalSpecimen.digitalSpecimenWrapper().attributes().setOdsHasEntityRelationship(erList);
  }

  private EntityRelationship createInterimMediaEr(DigitalMediaEventWithoutDOI digitalMedia) {
    return new EntityRelationship()
        .withType("ods:EntityRelationship")
        .withDwcRelationshipOfResource(MEDIA_RELATIONSHIP)
        .withDwcRelatedResourceID(
            digitalMedia.digitalMediaObjectWithoutDoi().attributes().getAcAccessURI())
        .withDwcRelationshipAccordingTo(fdoProperties.getApplicationName())
        .withOdsRelatedResourceURI(getUri(digitalMedia.digitalMediaObjectWithoutDoi()))
        .withOdsRelationshipAccordingToAgent(buildDiSSCoAgent())
        .withDwcRelationshipEstablishedDate(java.util.Date.from(Instant.now()))
        .withDwcRelationshipRemarks(
            "Media Object is not yet ingested in DiSSCo. PID is not yet available.");
  }

  private Agent buildDiSSCoAgent() {
    return new Agent()
        .withType(Agent.Type.AS_APPLICATION)
        .withId(fdoProperties.getApplicationPID())
        .withSchemaName(fdoProperties.getApplicationName());
  }

  private URI getUri(DigitalMediaWithoutDOI digitalMedia) {
    try {
      return new URI(digitalMedia.attributes().getAcAccessURI());
    } catch (URISyntaxException e) {
      log.error("Invalid URI received for digitalMedia: {}",
          digitalMedia.attributes().getAcAccessURI(), e);
      return null;
    }
  }

  private Optional<EntityRelationship> getErFromURI(DigitalSpecimenRecord digitalSpecimenRecord,
      DigitalMediaUpdatePidEvent updatePidEvent) {
    return digitalSpecimenRecord.digitalSpecimenWrapper().attributes().getOdsHasEntityRelationship()
        .stream()
        .filter(this::isDisscoMediaEr)
        .filter(er -> er.getDwcRelatedResourceID().equals(updatePidEvent.digitalMediaAccessURI()))
        .findFirst();
  }

}
