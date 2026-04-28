package eu.dissco.core.digitalspecimenprocessor.service.rabbitmqconsumer;

import eu.dissco.core.digitalspecimenprocessor.Profiles;
import eu.dissco.core.digitalspecimenprocessor.domain.relation.DigitalMediaRelationshipTombstoneEvent;
import eu.dissco.core.digitalspecimenprocessor.service.preprocessing.ErPreprocessingService;
import java.util.List;
import java.util.Objects;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.context.annotation.Profile;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;
import tools.jackson.databind.json.JsonMapper;

@Service
@Slf4j
@Profile(Profiles.ER_RABBIT_MQ)
@RequiredArgsConstructor
public class ErRabbitMqConsumerService {

	private final JsonMapper mapper;

	private final ErPreprocessingService processingService;

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
