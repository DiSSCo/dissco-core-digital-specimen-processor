package eu.dissco.core.digitalspecimenprocessor.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import eu.dissco.core.digitalspecimenprocessor.domain.SpecimenProcessResult;
import eu.dissco.core.digitalspecimenprocessor.domain.mas.MasJobRequest;
import eu.dissco.core.digitalspecimenprocessor.domain.mas.MjrTargetType;
import eu.dissco.core.digitalspecimenprocessor.domain.mas.SourceSystemMass;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaRecord;
import eu.dissco.core.digitalspecimenprocessor.domain.media.MediaProcessResult;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenRecord;
import eu.dissco.core.digitalspecimenprocessor.exception.DisscoJsonBMappingException;
import eu.dissco.core.digitalspecimenprocessor.property.ApplicationProperties;
import eu.dissco.core.digitalspecimenprocessor.repository.SourceSystemRepository;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class MasSchedulerService {

  private final RabbitMqPublisherService publisherService;
  private final SourceSystemRepository sourceSystemRepository;
  private final ApplicationProperties applicationProperties;

  public void scheduleMasSpecimen(List<DigitalSpecimenRecord> specimenRecords,
      SpecimenProcessResult processResult) {
    var newPids = new HashSet<>(processResult.newSpecimenPids().values());
    if (newPids.isEmpty()) {
      return;
    }
    var newSpecimens = specimenRecords.stream()
        .filter(specimenRecord -> newPids.contains(specimenRecord.id()))
        .toList();
    log.info("Scheduling MASs on {} new specimens", newSpecimens.size());
    var sourceSystemIds = newSpecimens.stream()
        .map(specimenRecord -> specimenRecord.digitalSpecimenWrapper().attributes()
            .getOdsSourceSystemID())
        .map(MasSchedulerService::removeHandlePrefix)
        .collect(Collectors.toSet());
    Map<String, SourceSystemMass> sourceSystemMass;
    try {
      sourceSystemMass = sourceSystemRepository.getSourceSystemMass(sourceSystemIds);
    } catch (DisscoJsonBMappingException e) {
      log.error("Unable to schedule MASs for specimens");
      return;
    }
    for (var specimen : newSpecimens) {
      var sourceSystemId = removeHandlePrefix(
          specimen.digitalSpecimenWrapper().attributes().getOdsSourceSystemID());
      var specimenMas = sourceSystemMass.get(sourceSystemId).specimenMass();
      for (var masId : specimenMas) {
        var masJobRequest = new MasJobRequest(
            masId,
            specimen.id(),
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


  public void scheduleMasMedia(List<DigitalMediaRecord> mediaRecords,
      MediaProcessResult processResult) {
    var newMediaUris = processResult.newDigitalMedia().stream()
        .map(m -> m.digitalMediaWrapper().attributes().getAcAccessURI()).collect(
            Collectors.toSet());
    if (newMediaUris.isEmpty()) {
      return;
    }
    log.info("Scheduling MASs on {} new media", newMediaUris.size());
    var newMediaRecords = mediaRecords.stream().filter(mediaRecord -> newMediaUris.contains(
        mediaRecord.accessURI()
    )).toList();

    var sourceSystemIds = newMediaRecords.stream()
        .map(mediaRecord -> mediaRecord.attributes()
            .getOdsSourceSystemID())
        .map(MasSchedulerService::removeHandlePrefix)
        .collect(Collectors.toSet());
    Map<String, SourceSystemMass> sourceSystemMass;
    try {
      sourceSystemMass = sourceSystemRepository.getSourceSystemMass(sourceSystemIds);
    } catch (DisscoJsonBMappingException e) {
      log.error("Unable to schedule MASs for specimens");
      return;
    }
    for (var media : newMediaRecords) {
      var sourceSystemId = removeHandlePrefix(media.attributes().getOdsSourceSystemID());
      var mediaMas = sourceSystemMass.get(sourceSystemId).mediaMass();
      for (var masId : mediaMas) {
        var masJobRequest = new MasJobRequest(
            masId,
            media.id(),
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

  private static String removeHandlePrefix(String id) {
    return id.replace("https://hdl.handle.net/", "");
  }

}
