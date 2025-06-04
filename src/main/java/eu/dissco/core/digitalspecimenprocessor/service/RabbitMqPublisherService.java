package eu.dissco.core.digitalspecimenprocessor.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dissco.core.digitalspecimenprocessor.domain.AutoAcceptedAnnotation;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaEvent;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaRecord;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenEvent;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenRecord;
import eu.dissco.core.digitalspecimenprocessor.property.RabbitMqProperties;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@AllArgsConstructor
public class RabbitMqPublisherService {

  private final ObjectMapper mapper;
  private final ProvenanceService provenanceService;
  private final RabbitTemplate rabbitTemplate;
  private final RabbitMqProperties rabbitMqProperties;

  public void publishCreateEventSpecimen(DigitalSpecimenRecord digitalSpecimenRecord) throws JsonProcessingException {
    var event = provenanceService.generateCreateEventSpecimen(digitalSpecimenRecord);
    rabbitTemplate.convertAndSend(rabbitMqProperties.getCreateUpdateTombstone().getExchangeName(),
        rabbitMqProperties.getCreateUpdateTombstone().getRoutingKeyName(),
        mapper.writeValueAsString(event));
  }

  public void publishCreateEventMedia(DigitalMediaRecord digitalMediaRecord)
      throws JsonProcessingException {
    var event = provenanceService.generateCreateEventMedia(digitalMediaRecord);
    rabbitTemplate.convertAndSend(rabbitMqProperties.getCreateUpdateTombstone().getExchangeName(),
        rabbitMqProperties.getCreateUpdateTombstone().getRoutingKeyName(),
        mapper.writeValueAsString(event));
  }

  public void publishAnnotationRequestEventSpecimen(String enrichmentRoutingKey,
      DigitalSpecimenRecord digitalSpecimenRecord) throws JsonProcessingException {
    rabbitTemplate.convertAndSend(rabbitMqProperties.getMasExchangeName(),
        enrichmentRoutingKey, mapper.writeValueAsString(digitalSpecimenRecord));
  }

  public void publishAnnotationRequestEventMedia(String enrichmentRoutingKey,
      DigitalMediaRecord digitalMediaRecord) throws JsonProcessingException {
    rabbitTemplate.convertAndSend(rabbitMqProperties.getMasExchangeName(),
        enrichmentRoutingKey, mapper.writeValueAsString(digitalMediaRecord));
  }

  public void publishUpdateEventSpecimen(DigitalSpecimenRecord digitalSpecimenRecord,
      JsonNode jsonPatch) throws JsonProcessingException {
    var event = provenanceService.generateUpdateEventSpecimen(digitalSpecimenRecord, jsonPatch);
    rabbitTemplate.convertAndSend(rabbitMqProperties.getCreateUpdateTombstone().getExchangeName(),
        rabbitMqProperties.getCreateUpdateTombstone().getRoutingKeyName(),
        mapper.writeValueAsString(event));
  }
    public void publishUpdateEventMedia(DigitalMediaRecord digitalMediaRecord,
      JsonNode jsonPatch) throws JsonProcessingException {
      var event = provenanceService.generateUpdateEventMedia(digitalMediaRecord, jsonPatch);
      rabbitTemplate.convertAndSend(rabbitMqProperties.getCreateUpdateTombstone().getExchangeName(),
        rabbitMqProperties.getCreateUpdateTombstone().getRoutingKeyName(),
        mapper.writeValueAsString(event));
  }

  public void republishSpecimenEvent(DigitalSpecimenEvent event) throws JsonProcessingException {
    rabbitTemplate.convertAndSend(rabbitMqProperties.getSpecimen().getExchangeName(),
        rabbitMqProperties.getSpecimen().getRoutingKeyName(), mapper.writeValueAsString(event));
  }

  public void republishMediaEvent(DigitalMediaEvent event) throws JsonProcessingException {
    rabbitTemplate.convertAndSend(rabbitMqProperties.getDigitalMedia().getExchangeName(),
        rabbitMqProperties.getDigitalMedia().getRoutingKeyName(), mapper.writeValueAsString(event));
  }

  public void deadLetterEventSpecimen(DigitalSpecimenEvent event) throws JsonProcessingException {
    rabbitTemplate.convertAndSend(rabbitMqProperties.getSpecimen().getDlqExchangeName(),
        rabbitMqProperties.getSpecimen().getDlqRoutingKeyName(), mapper.writeValueAsString(event));
  }

  public void deadLetterEventMedia(DigitalMediaEvent event) throws JsonProcessingException {
    rabbitTemplate.convertAndSend(rabbitMqProperties.getDigitalMedia().getDlqExchangeName(),
        rabbitMqProperties.getDigitalMedia().getDlqRoutingKeyName(), mapper.writeValueAsString(event));
  }


  public void deadLetterRaw(String event) {
    rabbitTemplate.convertAndSend(rabbitMqProperties.getSpecimen().getDlqExchangeName(),
        rabbitMqProperties.getSpecimen().getDlqRoutingKeyName(), event);
  }

  public void publishAcceptedAnnotation(AutoAcceptedAnnotation annotation)
      throws JsonProcessingException {
    rabbitTemplate.convertAndSend(rabbitMqProperties.getAutoAcceptedAnnotation().getExchangeName(),
        rabbitMqProperties.getAutoAcceptedAnnotation().getRoutingKeyName(),
        mapper.writeValueAsString(annotation));
  }

}
