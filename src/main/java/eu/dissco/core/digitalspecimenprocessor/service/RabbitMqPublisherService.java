package eu.dissco.core.digitalspecimenprocessor.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dissco.core.digitalspecimenprocessor.Profiles;
import eu.dissco.core.digitalspecimenprocessor.domain.AutoAcceptedAnnotation;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaEvent;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaRecord;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenEvent;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenRecord;
import eu.dissco.core.digitalspecimenprocessor.property.RabbitMqProperties;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@Profile(Profiles.RABBIT_MQ)
@AllArgsConstructor
public class RabbitMqPublisherService {

  private final ObjectMapper mapper;
  private final ProvenanceService provenanceService;
  private final RabbitTemplate rabbitTemplate;
  private final RabbitMqProperties rabbitMQProperties;

  public void publishCreateEventSpecimen(DigitalSpecimenRecord digitalSpecimenRecord)
      throws JsonProcessingException {
    var event = provenanceService.generateCreateEventSpecimen(digitalSpecimenRecord);
    rabbitTemplate.convertAndSend(rabbitMQProperties.getCreateUpdateTombstone().getExchangeName(),
        rabbitMQProperties.getCreateUpdateTombstone().getRoutingKeyName(),
        mapper.writeValueAsString(event));
  }

  public void publishCreateEventMedia(DigitalMediaRecord digitalMediaRecord)
      throws JsonProcessingException {
    var event = provenanceService.generateCreateEventMedia(digitalMediaRecord); // Todo
    rabbitTemplate.convertAndSend(rabbitMQProperties.getCreateUpdateTombstone().getExchangeName(),
        rabbitMQProperties.getCreateUpdateTombstone().getRoutingKeyName(),
        mapper.writeValueAsString(event));
  }

  public void publishAnnotationRequestEventSpecimen(String enrichmentRoutingKey,
      DigitalSpecimenRecord digitalSpecimenRecord) throws JsonProcessingException {
    rabbitTemplate.convertAndSend(rabbitMQProperties.getMasExchangeName(),
        enrichmentRoutingKey, mapper.writeValueAsString(digitalSpecimenRecord));
  }

  public void publishAnnotationRequestEventMedia(String enrichmentRoutingKey,
      DigitalMediaRecord digitalMediaRecord) throws JsonProcessingException {
    rabbitTemplate.convertAndSend(rabbitMQProperties.getMasExchangeName(),
        enrichmentRoutingKey, mapper.writeValueAsString(digitalMediaRecord));
  }

  public void publishUpdateEventSpecimen(DigitalSpecimenRecord digitalSpecimenRecord,
      JsonNode jsonPatch) throws JsonProcessingException {
    var event = provenanceService.generateUpdateEventSpecimen(digitalSpecimenRecord, jsonPatch);
    rabbitTemplate.convertAndSend(rabbitMQProperties.getCreateUpdateTombstone().getExchangeName(),
        rabbitMQProperties.getCreateUpdateTombstone().getRoutingKeyName(),
        mapper.writeValueAsString(event));
  }

  public void publishUpdateEventMedia(DigitalMediaRecord digitalMediaRecord,
      JsonNode jsonPatch) throws JsonProcessingException {
    var event = provenanceService.generateUpdateEventMedia(digitalMediaRecord, jsonPatch);
    rabbitTemplate.convertAndSend(rabbitMQProperties.getCreateUpdateTombstone().getExchangeName(),
        rabbitMQProperties.getCreateUpdateTombstone().getRoutingKeyName(),
        mapper.writeValueAsString(event));
  }

  public void republishSpecimenEvent(DigitalSpecimenEvent event) throws JsonProcessingException {
    rabbitTemplate.convertAndSend(rabbitMQProperties.getSpecimen().getExchangeName(),
        rabbitMQProperties.getSpecimen().getRoutingKeyName(), mapper.writeValueAsString(event));
  }

  // TOdo create pathway for just media
  public void republishMediaEvent(DigitalMediaEvent event) throws JsonProcessingException {
    rabbitTemplate.convertAndSend(rabbitMQProperties.getDigitalMedia().getExchangeName(),
        rabbitMQProperties.getDigitalMedia().getRoutingKeyName(), mapper.writeValueAsString(event));
  }

  public void deadLetterEventSpecimen(DigitalSpecimenEvent event) throws JsonProcessingException {
    rabbitTemplate.convertAndSend(rabbitMQProperties.getSpecimen().getDlqExchangeName(),
        rabbitMQProperties.getSpecimen().getDlqRoutingKeyName(), mapper.writeValueAsString(event));
  }

  public void deadLetterEventMedia(DigitalMediaEvent event) throws JsonProcessingException {
    rabbitTemplate.convertAndSend(rabbitMQProperties.getDigitalMedia().getDlqExchangeName(),
        rabbitMQProperties.getDigitalMedia().getDlqRoutingKeyName(), mapper.writeValueAsString(event));
  }


  public void deadLetterRaw(String event) {
    rabbitTemplate.convertAndSend(rabbitMQProperties.getSpecimen().getDlqExchangeName(),
        rabbitMQProperties.getSpecimen().getDlqRoutingKeyName(), event);
  }

  public void publishDigitalMediaObject(DigitalMediaEvent digitalMediaObjectEvent)
      throws JsonProcessingException {
    rabbitTemplate.convertAndSend(rabbitMQProperties.getDigitalMedia().getExchangeName(),
        rabbitMQProperties.getDigitalMedia().getRoutingKeyName(),
        mapper.writeValueAsString(digitalMediaObjectEvent));
  }

  public void publishAcceptedAnnotation(AutoAcceptedAnnotation annotation)
      throws JsonProcessingException {
    rabbitTemplate.convertAndSend(rabbitMQProperties.getAutoAcceptedAnnotation().getExchangeName(),
        rabbitMQProperties.getAutoAcceptedAnnotation().getRoutingKeyName(),
        mapper.writeValueAsString(annotation));
  }

}
