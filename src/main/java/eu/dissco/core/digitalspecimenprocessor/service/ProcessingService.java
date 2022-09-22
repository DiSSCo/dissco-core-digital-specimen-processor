package eu.dissco.core.digitalspecimenprocessor.service;

import eu.dissco.core.digitalspecimenprocessor.domain.DigitalSpecimen;
import eu.dissco.core.digitalspecimenprocessor.domain.DigitalSpecimenEvent;
import eu.dissco.core.digitalspecimenprocessor.domain.DigitalSpecimenRecord;
import eu.dissco.core.digitalspecimenprocessor.repository.DigitalSpecimenRepository;
import eu.dissco.core.digitalspecimenprocessor.repository.ElasticSearchRepository;
import java.time.Instant;
import java.util.List;
import javax.xml.transform.TransformerException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class ProcessingService {

  private static final int SUCCESS = 1;

  private final DigitalSpecimenRepository repository;
  private final HandleService handleService;
  private final ElasticSearchRepository elasticRepository;
  private final KafkaPublisherService kafkaService;

  public DigitalSpecimenRecord handleMessages(DigitalSpecimenEvent event)
      throws TransformerException {
    var digitalSpecimen = event.digitalSpecimen();
    log.info("ds: {}", digitalSpecimen);
    var currentDigitalSpecimenOptional = repository.getDigitalSpecimen(
        digitalSpecimen.physicalSpecimenId());
    if (currentDigitalSpecimenOptional.isEmpty()) {
      log.info("Specimen with id: {} is completely new", digitalSpecimen.physicalSpecimenId());
      return persistNewDigitalSpecimen(digitalSpecimen, event.enrichmentList());
    } else {
      var currentDigitalSpecimen = currentDigitalSpecimenOptional.get();
      if (currentDigitalSpecimen.digitalSpecimen().equals(digitalSpecimen)) {
        log.info("Received digital specimen is equal to digital specimen: {}",
            currentDigitalSpecimen.id());
        processEqualDigitalSpecimen(currentDigitalSpecimen);
        return null;
      } else {
        log.info("Specimen with id: {} has received an update", currentDigitalSpecimen.id());
        return updateExistingDigitalSpecimen(currentDigitalSpecimen, digitalSpecimen);
      }
    }
  }

  private void processEqualDigitalSpecimen(DigitalSpecimenRecord currentDigitalSpecimen) {
    var result = repository.updateLastChecked(currentDigitalSpecimen);
    if (result == SUCCESS) {
      log.info("Successfully updated lastChecked for existing digitalSpecimen: {}",
          currentDigitalSpecimen.id());
    }
  }

  private DigitalSpecimenRecord updateExistingDigitalSpecimen(
      DigitalSpecimenRecord currentDigitalSpecimen,
      DigitalSpecimen digitalSpecimen) {
    if (handleNeedsUpdate(currentDigitalSpecimen.digitalSpecimen(), digitalSpecimen)) {
      handleService.updateHandle(currentDigitalSpecimen.id(), digitalSpecimen);
    }
    var id = currentDigitalSpecimen.id();
    var midsLevel = calculateMidsLevel(digitalSpecimen);
    var version = currentDigitalSpecimen.version() + 1;
    var digitalSpecimenRecord = new DigitalSpecimenRecord(id, midsLevel, version, Instant.now(),
        digitalSpecimen);
    var result = repository.createDigitalSpecimenRecord(digitalSpecimenRecord);
    if (result == SUCCESS) {
      log.info("Specimen: {} has been successfully updated in the database", id);
      var indexDocument = elasticRepository.indexDigitalSpecimen(digitalSpecimenRecord);
      if (indexDocument.result().jsonValue().equals("updated")) {
        log.info("Specimen: {} has been successfully indexed", id);
        kafkaService.publishUpdateEvent(currentDigitalSpecimen, digitalSpecimenRecord);
      }
    }
    log.info("Successfully updated digital specimen with id: {}", id);
    return digitalSpecimenRecord;
  }

  private boolean handleNeedsUpdate(DigitalSpecimen currentDigitalSpecimen,
      DigitalSpecimen digitalSpecimen) {
    return !currentDigitalSpecimen.type().equals(digitalSpecimen.type()) ||
        !currentDigitalSpecimen.organizationId().equals(digitalSpecimen.organizationId());
  }

  private DigitalSpecimenRecord persistNewDigitalSpecimen(DigitalSpecimen digitalSpecimen,
      List<String> enrichmentList)
      throws TransformerException {
    var id = handleService.createNewHandle(digitalSpecimen);
    log.info("New id has been generated: {}", id);
    var midsLevel = calculateMidsLevel(digitalSpecimen);
    var digitalSpecimenRecord = new DigitalSpecimenRecord(id, midsLevel, 1, Instant.now(),
        digitalSpecimen);
    var result = repository.createDigitalSpecimenRecord(digitalSpecimenRecord);
    if (result == SUCCESS) {
      log.info("Specimen: {} has been successfully committed to database", id);
      var indexDocument = elasticRepository.indexDigitalSpecimen(digitalSpecimenRecord);
      if (indexDocument.result().jsonValue().equals("created")) {
        log.info("Specimen: {} has been successfully indexed", id);
        kafkaService.publishCreateEvent(digitalSpecimenRecord);
        for (var enrichment : enrichmentList) {
          kafkaService.publishAnnotationRequestEvent(enrichment, digitalSpecimenRecord);
        }
      }
    }
    log.info("Successfully created digital specimen with id: {}", id);
    return digitalSpecimenRecord;
  }

  private int calculateMidsLevel(DigitalSpecimen digitalSpecimen) {
    return 1;
  }
}
