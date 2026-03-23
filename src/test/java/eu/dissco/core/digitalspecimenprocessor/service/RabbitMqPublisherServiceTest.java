package eu.dissco.core.digitalspecimenprocessor.service;

import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MAPPER;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenAutoAcceptedAnnotation;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalMediaEvent;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalMediaEventWithRelationship;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalMediaTombstoneEvent;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenEvent;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenRecord;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenJsonPatchSpecimen;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenMasJobRequestMedia;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenNewAcceptedAnnotation;
import static org.assertj.core.api.Assertions.assertThat;

import eu.dissco.core.digitalspecimenprocessor.domain.AutoAcceptedAnnotation;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaEvent;
import eu.dissco.core.digitalspecimenprocessor.domain.relation.DigitalMediaRelationshipTombstoneEvent;
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
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.rabbitmq.RabbitMQContainer;

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
		declareRabbitResources("digital-specimen-exchange", "digital-specimen-queue", "digital-specimen", "direct");
		// Declare dlq exchange, queue and binding
		declareRabbitResources("digital-specimen-exchange-dlq", "digital-specimen-queue-dlq", "digital-specimen-dlq",
				"direct");
		// Declare digital media exchange, queue and binding
		declareRabbitResources("digital-media-exchange", "digital-media-queue", "digital-media", "direct");
		// Declare auto annotation exchange, queue and binding
		declareRabbitResources("auto-accepted-annotation-exchange", "auto-accepted-annotation-queue",
				"auto-accepted-annotation", "direct");
		// Declare create update tombstone exchange, queue and binding
		declareRabbitResources("provenance-exchange", "provenance-queue", "provenance.#", "topic");
		// Declare mas ocr exchange, queue and binding
		declareRabbitResources("mas-exchange", "mas-ocr-queue", "OCR", "direct");
		declareRabbitResources("mas-scheduler-exchange", "mas-scheduler-queue", "mas-scheduler", "direct");
		declareRabbitResources("digital-media-relationship-tombstone-exchange",
				"digital-media-relationship-tombstone-queue", "digital-media-relationship-tombstone", "direct");
		declareRabbitResources("digital-media-relationship-tombstone-exchange-dlq",
				"digital-media-relationship-tombstone-queue-dlq", "digital-media-relationship-tombstone-dlq", "direct");

		CachingConnectionFactory factory = new CachingConnectionFactory(container.getHost());
		factory.setPort(container.getAmqpPort());
		factory.setUsername(container.getAdminUsername());
		factory.setPassword(container.getAdminPassword());
		rabbitTemplate = new RabbitTemplate(factory);
		rabbitTemplate.setReceiveTimeout(100L);
	}

	private static void declareRabbitResources(String exchangeName, String queueName, String routingKey,
			String exchangeType) throws IOException, InterruptedException {
		container.execInContainer("rabbitmqadmin", "declare", "exchange", "name=" + exchangeName,
				"type=" + exchangeType, "durable=true");
		container.execInContainer("rabbitmqadmin", "declare", "queue", "name=" + queueName, "queue_type=quorum",
				"durable=true");
		container.execInContainer("rabbitmqadmin", "declare", "binding", "source=" + exchangeName,
				"destination_type=queue", "destination=" + queueName, "routing_key=" + routingKey);
	}

	@AfterAll
	static void shutdownContainer() {
		container.stop();
	}

	@BeforeEach
	void setup() {
		rabbitMqPublisherService = new RabbitMqPublisherService(MAPPER, provenanceService, rabbitTemplate,
				new RabbitMqProperties());
	}

	@Test
	void testPublishCreateEvent() {
		// Given

		// When
		rabbitMqPublisherService.publishCreateEventSpecimen(givenDigitalSpecimenRecord());

		// Then
		var message = rabbitTemplate.receive("provenance-queue");
		assertThat(new String(message.getBody())).isNotNull();
	}

	@Test
	void testPublishUpdateEvent() {
		// Given

		// When
		rabbitMqPublisherService.publishUpdateEventSpecimen(givenDigitalSpecimenRecord(2, false),
				givenJsonPatchSpecimen());

		// Then
		var dlqMessage = rabbitTemplate.receive("provenance-queue");
		assertThat(new String(dlqMessage.getBody())).isNotNull();
	}

	@Test
	void testRepublishEvent() {
		// Given
		var message = givenDigitalSpecimenEvent();

		// When
		rabbitMqPublisherService.republishSpecimenEvent(message);

		// Then
		var result = rabbitTemplate.receive("digital-specimen-queue");
		assertThat(MAPPER.readValue(new String(result.getBody()), DigitalSpecimenEvent.class)).isEqualTo(message);
	}

	@Test
	void testPublishMasJob() {
		// Given

		// When
		rabbitMqPublisherService.publishMasJobRequest(givenMasJobRequestMedia());

		// Then
		var result = rabbitTemplate.receive("mas-scheduler-queue");
		assertThat(result.getBody()).isNotEmpty();
	}

	@Test
	void testRepublishEventMedia() {
		// Given
		var message = givenDigitalMediaEvent();

		// When
		rabbitMqPublisherService.republishMediaEvent(message);

		// Then
		var result = rabbitTemplate.receive("digital-media-queue");
		assertThat(MAPPER.readValue(new String(result.getBody()), DigitalMediaEvent.class)).isEqualTo(message);
	}

	@Test
	void testDeadLetterEvent() {
		// Given
		var message = givenDigitalSpecimenEvent();

		// When
		rabbitMqPublisherService.deadLetterEventSpecimen(message);

		// Then
		var result = rabbitTemplate.receive("digital-specimen-queue-dlq");
		assertThat(MAPPER.readValue(new String(result.getBody()), DigitalSpecimenEvent.class)).isEqualTo(message);
	}

	@Test
	void testPublishDigitalMediaObjectEvent() {
		// Given
		var message = givenDigitalMediaEventWithRelationship();

		// When
		rabbitMqPublisherService.republishMediaEvent(message);

		// Then
		var result = rabbitTemplate.receive("digital-media-queue");
		assertThat(MAPPER.readValue(new String(result.getBody()), DigitalMediaEvent.class)).isEqualTo(message);
	}

	@Test
	void testPublishAcceptedAnnotation() {
		// Given
		var message = givenAutoAcceptedAnnotation(givenNewAcceptedAnnotation());

		// When
		rabbitMqPublisherService.publishAcceptedAnnotation(message);

		// Then
		var result = rabbitTemplate.receive("auto-accepted-annotation-queue");
		assertThat(MAPPER.readValue(new String(result.getBody()), AutoAcceptedAnnotation.class)).isEqualTo(message);
	}

	@Test
	void testPublishDigitalMediaRelationTombstone() {
		// Given
		var message = givenDigitalMediaTombstoneEvent();

		// When
		rabbitMqPublisherService.publishDigitalMediaRelationTombstone(message);

		// Then
		var result = rabbitTemplate.receive("digital-media-relationship-tombstone-queue");
		assertThat(MAPPER.readValue(new String(result.getBody()), DigitalMediaRelationshipTombstoneEvent.class))
			.isEqualTo(message);
	}

	@Test
	void testDeadLetterRawDigitalMediaRelationTombstone() {
		// Given
		var message = MAPPER.writeValueAsString(givenDigitalMediaTombstoneEvent());

		// When
		rabbitMqPublisherService.deadLetterRawDigitalMediaRelationshipTombstone(message);

		// Then
		var result = rabbitTemplate.receive("digital-media-relationship-tombstone-queue-dlq");
		assertThat(MAPPER.readValue(new String(result.getBody()), DigitalMediaRelationshipTombstoneEvent.class))
			.isEqualTo(givenDigitalMediaTombstoneEvent());
	}

}
