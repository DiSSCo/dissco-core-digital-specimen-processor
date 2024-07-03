package eu.dissco.core.digitalspecimenprocessor.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dissco.core.digitalspecimenprocessor.domain.DigitalMediaEvent;
import eu.dissco.core.digitalspecimenprocessor.domain.DigitalSpecimenEvent;
import eu.dissco.core.digitalspecimenprocessor.domain.DigitalSpecimenRecord;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class KafkaPublisherService {

  private static final String DLQ_SUBJECT = "digital-specimen-dlq";

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
      DigitalSpecimenRecord currentDigitalSpecimen) throws JsonProcessingException {
    var event = provenanceService.generateUpdateEvent(digitalSpecimenRecord, currentDigitalSpecimen);
    kafkaTemplate.send("createUpdateDeleteTopic", mapper.writeValueAsString(event));
  }

  public void republishEvent(DigitalSpecimenEvent event) throws JsonProcessingException {
    kafkaTemplate.send("digital-specimen", mapper.writeValueAsString(event));
  }

  public void deadLetterEvent(DigitalSpecimenEvent event) throws JsonProcessingException {
    kafkaTemplate.send(DLQ_SUBJECT, mapper.writeValueAsString(event));
  }

  public void deadLetterRaw(String event) {
    kafkaTemplate.send(DLQ_SUBJECT, event);
  }


  public void publishDigitalMediaObject(DigitalMediaEvent digitalMediaObjectEvent)
      throws JsonProcessingException {
    kafkaTemplate.send("digital-media-object", mapper.writeValueAsString(digitalMediaObjectEvent));
  }
}
