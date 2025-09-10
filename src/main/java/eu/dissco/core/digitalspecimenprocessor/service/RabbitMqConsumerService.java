package eu.dissco.core.digitalspecimenprocessor.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dissco.core.digitalspecimenprocessor.Profiles;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaEvent;
import eu.dissco.core.digitalspecimenprocessor.domain.relation.DigitalMediaRelationshipTombstoneEvent;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenEvent;
import java.util.List;
import java.util.Objects;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.context.annotation.Profile;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@Profile(Profiles.RABBIT_MQ)
@AllArgsConstructor
public class RabbitMqConsumerService {

  private static final String DLQ_MESSAGE = "Moving message to DLQ, failed to parse event message";

  private final ObjectMapper mapper;
  private final ProcessingService processingService;
  private final RabbitMqPublisherService publisherService;

  @RabbitListener(queues = {
      "${rabbitmq.queue-name-specimen:digital-specimen-queue}"}, containerFactory = "consumerBatchContainerFactory")
  public void getMessages(@Payload List<String> messages) {
    var events = messages.stream().map(message -> {
      try {
        return mapper.readValue(message, DigitalSpecimenEvent.class);
      } catch (JsonProcessingException e) {
        log.error(DLQ_MESSAGE, e);
        publisherService.deadLetterRaw(message);
        return null;
      }
    }).filter(Objects::nonNull).toList();
    processingService.handleMessages(events);
  }

  @RabbitListener(queues = {
      "${rabbitmq.queue-name-media:digital-media-queue}"}, containerFactory = "consumerBatchContainerFactory")
  public void getMessagesMedia(@Payload List<String> messages) {
    var events = messages.stream().map(message -> {
      try {
        return mapper.readValue(message, DigitalMediaEvent.class);
      } catch (JsonProcessingException e) {
        log.error(DLQ_MESSAGE, e);
        publisherService.deadLetterRawMedia(message);
        return null;
      }
    }).filter(Objects::nonNull).toList();
    processingService.handleMessagesMedia(events);
  }

  @RabbitListener(queues = {
      "${rabbitmq.queue-name-media-relationship-tombstone:digital-media-relationship-tombstone-queue}"}, containerFactory = "consumerBatchContainerFactory")
  public void getMessagesDigitalMediaRelationshipTombstone(@Payload List<String> messages) {
    var events = messages.stream().map(message -> {
      try {
        return mapper.readValue(message, DigitalMediaRelationshipTombstoneEvent.class);
      } catch (JsonProcessingException e) {
        log.error(DLQ_MESSAGE, e);
        publisherService.deadLetterRawDigitalMediaRelationshipTombstone(message);
        return null;
      }
    }).filter(Objects::nonNull).toList();
    processingService.handleMessagesMediaRelationshipTombstone(events);
  }

}
