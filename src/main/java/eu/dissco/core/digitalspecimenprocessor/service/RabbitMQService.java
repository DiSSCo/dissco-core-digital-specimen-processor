package eu.dissco.core.digitalspecimenprocessor.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dissco.core.digitalspecimenprocessor.Profiles;
import eu.dissco.core.digitalspecimenprocessor.domain.AutoAcceptedAnnotation;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaEvent;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenEvent;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenRecord;
import eu.dissco.core.digitalspecimenprocessor.property.RabbitMQProperties;
import java.util.List;
import java.util.Objects;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.context.annotation.Profile;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@Profile(Profiles.RABBIT_MQ)
@AllArgsConstructor
public class RabbitMQService {

  private final ObjectMapper mapper;
  private final ProcessingService processingService;
  private final ProvenanceService provenanceService;
  private final RabbitTemplate rabbitTemplate;
  private final RabbitMQProperties rabbitMQProperties;

  @RabbitListener(queues = "#{rabbitMQProperties.specimenQueueName}", containerFactory = "consumerBatchContainerFactory")
  public void getMessages(@Payload List<String> messages) {
    var events = messages.stream().map(message -> {
      try {
        return mapper.readValue(message, DigitalSpecimenEvent.class);
      } catch (JsonProcessingException e) {
        log.error("Moving message to DLQ, failed to parse event message", e);
        deadLetterRaw(message);
        return null;
      }
    }).filter(Objects::nonNull).toList();
    processingService.handleMessages(events);
  }

  public void publishCreateEvent(DigitalSpecimenRecord digitalSpecimenRecord)
      throws JsonProcessingException {
    var event = provenanceService.generateCreateEvent(digitalSpecimenRecord);
    rabbitTemplate.convertAndSend(rabbitMQProperties.getCreateUpdateTombstoneExchangeName(),
        rabbitMQProperties.getCreateUpdateTombstoneRoutingKeyName(), mapper.writeValueAsString(event));
  }

  public void publishAnnotationRequestEvent(String enrichmentRoutingKey,
      DigitalSpecimenRecord digitalSpecimenRecord) throws JsonProcessingException {
    rabbitTemplate.convertAndSend(rabbitMQProperties.getMasExchangeName(),
        enrichmentRoutingKey, mapper.writeValueAsString(digitalSpecimenRecord));
  }

  public void publishUpdateEvent(DigitalSpecimenRecord digitalSpecimenRecord,
      JsonNode jsonPatch) throws JsonProcessingException {
    var event = provenanceService.generateUpdateEvent(digitalSpecimenRecord, jsonPatch);
    rabbitTemplate.convertAndSend(rabbitMQProperties.getCreateUpdateTombstoneExchangeName(),
        rabbitMQProperties.getCreateUpdateTombstoneRoutingKeyName(), mapper.writeValueAsString(event));
  }

  public void republishEvent(DigitalSpecimenEvent event) throws JsonProcessingException {
    rabbitTemplate.convertAndSend(rabbitMQProperties.getSpecimenExchangeName(),
        rabbitMQProperties.getSpecimenRoutingKeyName(), mapper.writeValueAsString(event));
  }

  public void deadLetterEvent(DigitalSpecimenEvent event) throws JsonProcessingException {
    rabbitTemplate.convertAndSend(rabbitMQProperties.getSpecimenDlqExchangeName(),
        rabbitMQProperties.getSpecimenDlqRoutingKeyName(), mapper.writeValueAsString(event));
  }

  public void deadLetterRaw(String event) {
    rabbitTemplate.convertAndSend(rabbitMQProperties.getSpecimenDlqExchangeName(),
        rabbitMQProperties.getSpecimenDlqRoutingKeyName(), event);
  }

  public void publishDigitalMediaObject(DigitalMediaEvent digitalMediaObjectEvent)
      throws JsonProcessingException {
    rabbitTemplate.convertAndSend(rabbitMQProperties.getDigitalMediaExchangeName(),
        rabbitMQProperties.getDigitalMediaRoutingKeyName(), mapper.writeValueAsString(digitalMediaObjectEvent));
  }

  public void publishAcceptedAnnotation(AutoAcceptedAnnotation annotation)
      throws JsonProcessingException {
    rabbitTemplate.convertAndSend(rabbitMQProperties.getAutoAnnotationExchangeName(),
        rabbitMQProperties.getAutoAnnotationRoutingKeyName(), mapper.writeValueAsString(annotation));
  }

}
