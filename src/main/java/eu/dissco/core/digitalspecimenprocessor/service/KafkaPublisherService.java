package eu.dissco.core.digitalspecimenprocessor.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dissco.core.digitalspecimenprocessor.domain.AutoAcceptedAnnotation;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaEvent;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaEventWithoutDOI;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenEvent;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenRecord;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class KafkaPublisherService {

  private static final String DLQ_SUBJECT_SPECIMEN = "digital-specimen-dlq";
  private static final String DLQ_SUBJECT_MEDIA = "digital-media-dlq";


  private final ObjectMapper mapper;
  private final KafkaTemplate<String, String> kafkaTemplate;
  private final ProvenanceService provenanceService;

  public void publishCreateEvent(DigitalSpecimenRecord digitalSpecimenRecord)
      throws JsonProcessingException {
    var event = provenanceService.generateCreateEvent(digitalSpecimenRecord);
    kafkaTemplate.send("createUpdateDeleteTopic", mapper.writeValueAsString(event));
  }

  public void publishAnnotationRequestEvent(String enrichment,
      DigitalSpecimenRecord digitalSpecimenRecord) throws JsonProcessingException {
    kafkaTemplate.send(enrichment, mapper.writeValueAsString(digitalSpecimenRecord));
  }

  public void publishUpdateEvent(DigitalSpecimenRecord digitalSpecimenRecord,
      JsonNode jsonPatch) throws JsonProcessingException {
    var event = provenanceService.generateUpdateEventSpecimen(digitalSpecimenRecord, jsonPatch);
    kafkaTemplate.send("createUpdateDeleteTopic", mapper.writeValueAsString(event));
  }

  public void publishUpdateEventMedia(DigitalMediaEventWithoutDOI digitalMediaEvent,
      JsonNode jsonPatch) throws JsonProcessingException {
    var event = provenanceService.generateUpdateEventSpecimen(digitalMediaEvent, jsonPatch);
    kafkaTemplate.send("createUpdateDeleteTopic", mapper.writeValueAsString(event));
  }

  public void republishSpecimenEvent(DigitalSpecimenEvent event) throws JsonProcessingException {
    kafkaTemplate.send("digital-specimen", mapper.writeValueAsString(event));
  }

  public void republishMediaEvent(DigitalMediaEventWithoutDOI event) throws JsonProcessingException {
    kafkaTemplate.send("digital-media", mapper.writeValueAsString(event));
  }

  public void deadLetterEventSpecimen(DigitalSpecimenEvent event) throws JsonProcessingException {
    kafkaTemplate.send(DLQ_SUBJECT_SPECIMEN, mapper.writeValueAsString(event));
  }

  public void deadLetterEventMedia(DigitalMediaEventWithoutDOI event) throws JsonProcessingException {
    kafkaTemplate.send(DLQ_SUBJECT_MEDIA, mapper.writeValueAsString(event));
  }

  public void deadLetterRaw(String event) {
    kafkaTemplate.send(DLQ_SUBJECT_SPECIMEN, event);
  }


  public void publishDigitalMediaObject(DigitalMediaEvent digitalMediaObjectEvent)
      throws JsonProcessingException {
    kafkaTemplate.send("digital-media", mapper.writeValueAsString(digitalMediaObjectEvent));
  }

  public void publishAcceptedAnnotation(AutoAcceptedAnnotation annotation)
      throws JsonProcessingException {
    kafkaTemplate.send("auto-accepted-annotation",
        mapper.writeValueAsString(annotation));
  }
}
