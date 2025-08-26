package eu.dissco.core.digitalspecimenprocessor.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import eu.dissco.core.digitalspecimenprocessor.database.jooq.enums.MjrTargetType;
import eu.dissco.core.digitalspecimenprocessor.domain.mas.MasJobRequest;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaRecord;
import eu.dissco.core.digitalspecimenprocessor.domain.media.MediaProcessResult;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenRecord;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.SpecimenProcessResult;
import eu.dissco.core.digitalspecimenprocessor.property.ApplicationProperties;
import java.util.ArrayList;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class MasSchedulerService {

  private final RabbitMqPublisherService publisherService;
  private final ApplicationProperties applicationProperties;

  public void scheduleMasForSpecimen(SpecimenProcessResult processResult) {
    var recordsToSchedule = getDigitalSpecimenRecordsToSchedule(processResult);
    for (var specimenRecord : recordsToSchedule) {
      for (var masId : specimenRecord.masIds()) {
        publishMas(masId, specimenRecord.id(), MjrTargetType.DIGITAL_SPECIMEN);
      }
    }
  }

  public void scheduleMasForMedia(MediaProcessResult processResult) {
    var recordsToSchedule = getDigitalMediaRecordsToSchedule(processResult);
    for (var mediaRecord : recordsToSchedule) {
      for (var masId : mediaRecord.masIds()) {
        publishMas(masId, mediaRecord.id(), MjrTargetType.MEDIA_OBJECT);
      }
    }
  }

  private static List<DigitalMediaRecord> getDigitalMediaRecordsToSchedule(
      MediaProcessResult processResult) {
    var recordsToSchedule = new ArrayList<>(processResult.newMedia());
    recordsToSchedule.addAll(processResult.equalMedia().stream()
        .filter(DigitalMediaRecord::forceMasSchedule).toList());
    recordsToSchedule.addAll(processResult.updatedMedia().stream()
        .filter(DigitalMediaRecord::forceMasSchedule).toList());
    return recordsToSchedule.stream().filter(media -> !media.masIds().isEmpty()).toList();
  }

  private static List<DigitalSpecimenRecord> getDigitalSpecimenRecordsToSchedule(
      SpecimenProcessResult processResult) {
    var recordsToSchedule = new ArrayList<>(processResult.newDigitalSpecimens());
    recordsToSchedule.addAll(processResult.equalDigitalSpecimens().stream()
        .filter(DigitalSpecimenRecord::forceMasSchedule).toList());
    recordsToSchedule.addAll(processResult.updatedDigitalSpecimens().stream()
        .filter(DigitalSpecimenRecord::forceMasSchedule).toList());
    return recordsToSchedule.stream().filter(specimen -> !specimen.masIds().isEmpty()).toList();
  }

  private void publishMas(String masId, String targetId, MjrTargetType targetType) {
    var masJobRequest = new MasJobRequest(
        masId,
        targetId,
        false,
        applicationProperties.getPid(),
        targetType
    );
    try {
      publisherService.publishMasJobRequest(masJobRequest);
    } catch (JsonProcessingException e) {
      log.error("Unable to publish mas job request {}", masJobRequest);
    }
  }

}
