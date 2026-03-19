package eu.dissco.core.digitalspecimenprocessor.service;

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
import tools.jackson.databind.json.JsonMapper;

@Service
@Slf4j
@Profile(Profiles.RABBIT_MQ)
@AllArgsConstructor
public class RabbitMqConsumerService {

	private final JsonMapper mapper;

	private final ProcessingService processingService;

	@RabbitListener(queues = { "${rabbitmq.queue-name-specimen:digital-specimen-queue}" },
			containerFactory = "consumerBatchContainerFactory")
	public void getMessages(@Payload List<String> messages) {
		var events = messages.stream()
			.map(message -> mapper.readValue(message, DigitalSpecimenEvent.class))
			.filter(Objects::nonNull)
			.toList();
		processingService.handleMessages(events);
	}

	@RabbitListener(queues = { "${rabbitmq.queue-name-media:digital-media-queue}" },
			containerFactory = "consumerBatchContainerFactory")
	public void getMessagesMedia(@Payload List<String> messages) {
		var events = messages.stream()
			.map(message -> mapper.readValue(message, DigitalMediaEvent.class))
			.filter(Objects::nonNull)
			.toList();
		processingService.handleMessagesMedia(events);
	}

	@RabbitListener(
			queues = {
					"${rabbitmq.queue-name-media-relationship-tombstone:digital-media-relationship-tombstone-queue}" },
			containerFactory = "consumerBatchContainerFactory")
	public void getMessagesDigitalMediaRelationshipTombstone(@Payload List<String> messages) {
		var events = messages.stream()
			.map(message -> mapper.readValue(message, DigitalMediaRelationshipTombstoneEvent.class))
			.filter(Objects::nonNull)
			.toList();
		processingService.handleMessagesMediaRelationshipTombstone(events);
	}

}
