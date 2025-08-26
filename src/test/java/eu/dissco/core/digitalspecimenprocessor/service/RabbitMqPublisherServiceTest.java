package eu.dissco.core.digitalspecimenprocessor.service;

import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MAPPER;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenAutoAcceptedAnnotation;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalMediaEvent;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalMediaEventWithRelationship;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenEvent;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenRecord;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenJsonPatchSpecimen;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenMasJobRequestMedia;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenNewAcceptedAnnotation;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import eu.dissco.core.digitalspecimenprocessor.domain.AutoAcceptedAnnotation;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaEvent;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenEvent;
import eu.dissco.core.digitalspecimenprocessor.property.RabbitMqProperties;
import java.io.IOException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
@ExtendWith(MockitoExtension.class)
class RabbitMqPublisherServiceTest {

  private static RabbitMQContainer container;
  private static RabbitTemplate rabbitTemplate;
  private RabbitMqPublisherService rabbitMqPublisherService;
  @Mock
  private ProvenanceService provenanceService;

  @BeforeAll
  static void setupContainer() throws IOException, InterruptedException {
    container = new RabbitMQContainer("rabbitmq:4.0.8-management-alpine");
    container.start();
    // Declare digital specimen exchange, queue and binding
    declareRabbitResources("digital-specimen-exchange", "digital-specimen-queue",
        "digital-specimen");
    // Declare dlq exchange, queue and binding
    declareRabbitResources("digital-specimen-exchange-dlq", "digital-specimen-queue-dlq",
        "digital-specimen-dlq");
    // Declare digital media exchange, queue and binding
    declareRabbitResources("digital-media-exchange", "digital-media-queue", "digital-media");
    // Declare auto annotation exchange, queue and binding
    declareRabbitResources("auto-accepted-annotation-exchange", "auto-accepted-annotation-queue",
        "auto-accepted-annotation");
    // Declare create update tombstone exchange, queue and binding
    declareRabbitResources("create-update-tombstone-exchange", "create-update-tombstone-queue",
        "create-update-tombstone");
    // Declare mas ocr exchange, queue and binding
    declareRabbitResources("mas-exchange", "mas-ocr-queue", "OCR");
    declareRabbitResources("mas-scheduler-exchange", "mas-scheduler-queue", "mas-scheduler");


    CachingConnectionFactory factory = new CachingConnectionFactory(container.getHost());
    factory.setPort(container.getAmqpPort());
    factory.setUsername(container.getAdminUsername());
    factory.setPassword(container.getAdminPassword());
    rabbitTemplate = new RabbitTemplate(factory);
    rabbitTemplate.setReceiveTimeout(100L);
  }


  private static void declareRabbitResources(String exchangeName, String queueName,
      String routingKey)
      throws IOException, InterruptedException {
    container.execInContainer("rabbitmqadmin", "declare", "exchange", "name=" + exchangeName,
        "type=direct", "durable=true");
    container.execInContainer("rabbitmqadmin", "declare", "queue", "name=" + queueName,
        "queue_type=quorum", "durable=true");
    container.execInContainer("rabbitmqadmin", "declare", "binding", "source=" + exchangeName,
        "destination_type=queue", "destination=" + queueName, "routing_key=" + routingKey);
  }

  @AfterAll
  static void shutdownContainer() {
    container.stop();
  }

  @BeforeEach
  void setup() {
    rabbitMqPublisherService = new RabbitMqPublisherService(MAPPER, provenanceService,
        rabbitTemplate, new RabbitMqProperties());
  }

  @Test
  void testPublishCreateEvent() throws JsonProcessingException {
    // Given

    // When
    rabbitMqPublisherService.publishCreateEventSpecimen(givenDigitalSpecimenRecord());

    // Then
    var dlqMessage = rabbitTemplate.receive("create-update-tombstone-queue");
    assertThat(new String(dlqMessage.getBody())).isNotNull();
  }

  @Test
  void testPublishUpdateEvent() throws JsonProcessingException {
    // Given

    // When
    rabbitMqPublisherService.publishUpdateEventSpecimen(givenDigitalSpecimenRecord(2, false),
        givenJsonPatchSpecimen());

    // Then
    var dlqMessage = rabbitTemplate.receive("create-update-tombstone-queue");
    assertThat(new String(dlqMessage.getBody())).isNotNull();
  }

  @Test
  void testRepublishEvent() throws JsonProcessingException {
    // Given
    var message = givenDigitalSpecimenEvent();

    // When
    rabbitMqPublisherService.republishSpecimenEvent(message);

    // Then
    var result = rabbitTemplate.receive("digital-specimen-queue");
    assertThat(
        MAPPER.readValue(new String(result.getBody()), DigitalSpecimenEvent.class)).isEqualTo(
        message);
  }

  @Test
  void testPublishMasJob() throws JsonProcessingException {
    // Given

    // When
    rabbitMqPublisherService.publishMasJobRequest(givenMasJobRequestMedia());

    // Then
    var result = rabbitTemplate.receive("mas-scheduler-queue");
    assertThat(result.getBody()).isNotEmpty();
  }


  @Test
  void testRepublishEventMedia() throws JsonProcessingException {
    // Given
    var message = givenDigitalMediaEvent();

    // When
    rabbitMqPublisherService.republishMediaEvent(message);

    // Then
    var result = rabbitTemplate.receive("digital-media-queue");
    assertThat(
        MAPPER.readValue(new String(result.getBody()), DigitalMediaEvent.class)).isEqualTo(
        message);
  }

  @Test
  void testDeadLetterEvent() throws JsonProcessingException {
    // Given
    var message = givenDigitalSpecimenEvent();

    // When
    rabbitMqPublisherService.deadLetterEventSpecimen(message);

    // Then
    var result = rabbitTemplate.receive("digital-specimen-queue-dlq");
    assertThat(
        MAPPER.readValue(new String(result.getBody()), DigitalSpecimenEvent.class)).isEqualTo(
        message);
  }

  @Test
  void testPublishDigitalMediaObjectEvent() throws JsonProcessingException {
    // Given
    var message = givenDigitalMediaEventWithRelationship();

    // When
    rabbitMqPublisherService.republishMediaEvent(message);

    // Then
    var result = rabbitTemplate.receive("digital-media-queue");
    assertThat(
        MAPPER.readValue(new String(result.getBody()), DigitalMediaEvent.class)).isEqualTo(
        message);
  }

  @Test
  void testPublishAcceptedAnnotation() throws JsonProcessingException {
    // Given
    var message = givenAutoAcceptedAnnotation(givenNewAcceptedAnnotation());

    // When
    rabbitMqPublisherService.publishAcceptedAnnotation(message);

    // Then
    var result = rabbitTemplate.receive("auto-accepted-annotation-queue");
    assertThat(
        MAPPER.readValue(new String(result.getBody()), AutoAcceptedAnnotation.class)).isEqualTo(
        message);
  }
}
