package eu.dissco.core.digitalspecimenprocessor.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.fge.jsonpatch.diff.JsonDiff;
import eu.dissco.core.digitalspecimenprocessor.domain.CreateUpdateDeleteEvent;
import eu.dissco.core.digitalspecimenprocessor.domain.DigitalSpecimenRecord;
import java.time.Instant;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class KafkaPublisherService {

  private final ObjectMapper mapper;
  private final KafkaTemplate<String, String> kafkaTemplate;

  public void publishCreateEvent(DigitalSpecimenRecord digitalSpecimenRecord) {
    var event = new CreateUpdateDeleteEvent(UUID.randomUUID(), "create", "digital-specimen-processing-service",
        digitalSpecimenRecord.id(), Instant.now(), mapper.valueToTree(digitalSpecimenRecord),
        "Specimen newly created");
    try {
      kafkaTemplate.send("createUpdateDeleteTopic", mapper.writeValueAsString(event));
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  public void publishAnnotationRequestEvent(String enrichment,
      DigitalSpecimenRecord digitalSpecimenRecord) {
    try {
      kafkaTemplate.send(enrichment, mapper.writeValueAsString(digitalSpecimenRecord));
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  public void publishUpdateEvent(DigitalSpecimenRecord digitalSpecimenRecord,
      DigitalSpecimenRecord currentDigitalSpecimen) {
    var jsonPatch = createJsonPatch(currentDigitalSpecimen, digitalSpecimenRecord);
    var event = new CreateUpdateDeleteEvent(UUID.randomUUID(), "update", "processing-service",
        digitalSpecimenRecord.id(), Instant.now(), jsonPatch,
        "Specimen has been updated");
    try {
      kafkaTemplate.send("createUpdateDeleteTopic", mapper.writeValueAsString(event));
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  private JsonNode createJsonPatch(DigitalSpecimenRecord currentDigitalSpecimen,
      DigitalSpecimenRecord digitalSpecimenRecord) {
    return JsonDiff.asJson(mapper.valueToTree(currentDigitalSpecimen.digitalSpecimen()),
        mapper.valueToTree(digitalSpecimenRecord.digitalSpecimen()));
  }
}
