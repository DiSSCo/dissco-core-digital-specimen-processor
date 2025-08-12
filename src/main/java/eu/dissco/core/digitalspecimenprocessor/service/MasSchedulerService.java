package eu.dissco.core.digitalspecimenprocessor.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import eu.dissco.core.digitalspecimenprocessor.database.jooq.enums.MjrTargetType;
import eu.dissco.core.digitalspecimenprocessor.domain.SpecimenProcessResult;
import eu.dissco.core.digitalspecimenprocessor.domain.mas.MasJobRequest;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaEvent;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaRecord;
import eu.dissco.core.digitalspecimenprocessor.domain.media.MediaProcessResult;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenEvent;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenRecord;
import eu.dissco.core.digitalspecimenprocessor.property.ApplicationProperties;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class MasSchedulerService {

  private final RabbitMqPublisherService publisherService;
  private final ApplicationProperties applicationProperties;


  /*
  There are two conditions in which we want to schedule a MAS
  1. If the specimen is new
  2. If the event has the "force mas schedule" flag set to true
   */
  public void scheduleMasSpecimenFromEvent(Set<DigitalSpecimenEvent> specimenEvents,
      List<DigitalSpecimenRecord> digitalSpecimenRecords,
      SpecimenProcessResult specimenProcessResult) {
    var idMap = digitalSpecimenRecords.stream().collect(Collectors.toMap(
        specimen -> specimen.digitalSpecimenWrapper().physicalSpecimenID(),
        DigitalSpecimenRecord::id
    ));
    var newPhysicalSpecimenIds = specimenProcessResult.newSpecimens().stream()
        .map(event -> event.digitalSpecimenWrapper().physicalSpecimenID()).collect(
            Collectors.toSet());
    for (var event : specimenEvents) {
      if (!event.masList().isEmpty()
          && (Boolean.TRUE.equals(event.forceMasSchedule()) || isNewSpecimen(event,
          newPhysicalSpecimenIds))) { // Verify null still works
        var specimenId = idMap.get(event.digitalSpecimenWrapper().physicalSpecimenID());
        for (var masId : event.masList()) {
          var masJobRequest = new MasJobRequest(
              masId,
              specimenId,
              false,
              applicationProperties.getPid(),
              MjrTargetType.DIGITAL_SPECIMEN
          );
          try {
            publisherService.publishMasJobRequest(masJobRequest);
          } catch (JsonProcessingException e) {
            log.error("Unable to publish mas job request {}", masJobRequest);
          }
        }
      }
    }
  }

  public void scheduleMasMediaFromEvent(Set<DigitalMediaEvent> mediaEvents,
      List<DigitalMediaRecord> digitalMediaRecords,
      MediaProcessResult mediaProcessResult) {
    var idMap = digitalMediaRecords.stream().collect(Collectors.toMap(
        media -> media.attributes().getAcAccessURI(),
        DigitalMediaRecord::id
    ));
    var newMediaUris = mediaProcessResult.newDigitalMedia().stream()
        .map(event -> event.digitalMediaWrapper().attributes().getAcAccessURI()).collect(
            Collectors.toSet());
    for (var event : mediaEvents) {
      if (!event.masList().isEmpty()
          && (Boolean.TRUE.equals(event.forceMasSchedule()) || isNewMedia(event,
          newMediaUris))) { // Verify null still works
        var mediaId = idMap.get(event.digitalMediaWrapper().attributes().getAcAccessURI());
        for (var masId : event.masList()) {
          var masJobRequest = new MasJobRequest(
              masId,
              mediaId,
              false,
              applicationProperties.getPid(),
              MjrTargetType.MEDIA_OBJECT
          );
          try {
            publisherService.publishMasJobRequest(masJobRequest);
          } catch (JsonProcessingException e) {
            log.error("Unable to publish mas job request {}", masJobRequest);
          }
        }
      }
    }
  }

  private static boolean isNewSpecimen(DigitalSpecimenEvent specimenEvent,
      Set<String> newPhysicalSpecimenIds) {
    return newPhysicalSpecimenIds.contains(
        specimenEvent.digitalSpecimenWrapper().physicalSpecimenID());
  }

  private static boolean isNewMedia(DigitalMediaEvent mediaEvent, Set<String> newMediaUris) {
    return newMediaUris.contains(mediaEvent.digitalMediaWrapper().attributes().getAcAccessURI());
  }

}
