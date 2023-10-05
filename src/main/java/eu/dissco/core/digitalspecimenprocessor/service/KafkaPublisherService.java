package eu.dissco.core.digitalspecimenprocessor.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.fge.jsonpatch.diff.JsonDiff;
import eu.dissco.core.digitalspecimenprocessor.domain.CreateUpdateDeleteEvent;
import eu.dissco.core.digitalspecimenprocessor.domain.DigitalMediaObjectEvent;
import eu.dissco.core.digitalspecimenprocessor.domain.DigitalSpecimenEvent;
import eu.dissco.core.digitalspecimenprocessor.domain.DigitalSpecimenRecord;
import java.time.Instant;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class KafkaPublisherService {

  private static final String SUBJECT_TYPE = "DigitalSpecimen";
  private static final String DLQ_SUBJECT = "digital-specimen-dlq";

  private final ObjectMapper mapper;
  private final KafkaTemplate<String, String> kafkaTemplate;

  public void publishCreateEvent(DigitalSpecimenRecord digitalSpecimenRecord)
      throws JsonProcessingException {
    var event = new CreateUpdateDeleteEvent(UUID.randomUUID(),
        "create",
        "digital-specimen-processing-service",
        digitalSpecimenRecord.id(),
        SUBJECT_TYPE,
        Instant.now(),
        mapper.valueToTree(digitalSpecimenRecord),
        null,
        "Specimen newly created");
    kafkaTemplate.send("createUpdateDeleteTopic", mapper.writeValueAsString(event));
  }

  public void publishAnnotationRequestEvent(String enrichment,
      DigitalSpecimenRecord digitalSpecimenRecord) throws JsonProcessingException {
    kafkaTemplate.send(enrichment, mapper.writeValueAsString(digitalSpecimenRecord));
  }

  public void publishUpdateEvent(DigitalSpecimenRecord digitalSpecimenRecord,
      DigitalSpecimenRecord currentDigitalSpecimen) throws JsonProcessingException {
    var jsonPatch = createJsonPatch(currentDigitalSpecimen, digitalSpecimenRecord);
    var event = new CreateUpdateDeleteEvent(
        UUID.randomUUID(),
        "update",
        "processing-service",
        digitalSpecimenRecord.id(),
        SUBJECT_TYPE,
        Instant.now(),
        mapper.valueToTree(digitalSpecimenRecord),
        jsonPatch,
        "Specimen has been updated");
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

  private JsonNode createJsonPatch(DigitalSpecimenRecord currentDigitalSpecimen,
      DigitalSpecimenRecord digitalSpecimenRecord) {
    return JsonDiff.asJson(mapper.valueToTree(currentDigitalSpecimen.digitalSpecimenWrapper()),
        mapper.valueToTree(digitalSpecimenRecord.digitalSpecimenWrapper()));
  }

  public void publishDigitalMediaObject(DigitalMediaObjectEvent digitalMediaObjectEvent)
      throws JsonProcessingException {
    kafkaTemplate.send("digital-media-object", mapper.writeValueAsString(digitalMediaObjectEvent));
  }
}
