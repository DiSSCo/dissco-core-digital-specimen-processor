package eu.dissco.core.digitalspecimenprocessor.service.rabbitmqconsumer;

import eu.dissco.core.digitalspecimenprocessor.Profiles;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaEvent;
import eu.dissco.core.digitalspecimenprocessor.service.preprocessing.MediaPreprocessingService;
import java.util.List;
import java.util.Objects;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.context.annotation.Profile;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;
import tools.jackson.databind.ObjectMapper;

@Service
@Slf4j
@Profile(Profiles.MEDIA_RABBIT_MQ)
@RequiredArgsConstructor
public class MediaRabbitMqConsumerService {

	private final ObjectMapper mapper;

	private final MediaPreprocessingService processingService;

	@RabbitListener(queues = { "${rabbitmq.queue-name-media:digital-media-queue}" },
			containerFactory = "consumerBatchContainerFactory")
	public void getMessagesMedia(@Payload List<String> messages) {
		var events = messages.stream()
			.map(message -> mapper.readValue(message, DigitalMediaEvent.class))
			.filter(Objects::nonNull)
			.toList();
		processingService.handleMessagesMedia(events);
	}

}
