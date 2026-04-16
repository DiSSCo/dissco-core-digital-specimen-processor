package eu.dissco.core.digitalspecimenprocessor.service.rabbitmqconsumer;

import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MAPPER;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalMediaEvent;
import static org.mockito.BDDMockito.then;

import eu.dissco.core.digitalspecimenprocessor.service.preprocessing.MediaPreprocessingService;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class MediaRabbitMqConsumerServiceTest {

	@Mock
	private MediaPreprocessingService processingService;

	private MediaRabbitMqConsumerService consumerService;

	@BeforeEach
	void setup() {
		consumerService = new MediaRabbitMqConsumerService(MAPPER, processingService);
	}

	@Test
	void testGetMessagesMedia() {
		// Given
		var message = MAPPER.writeValueAsString(givenDigitalMediaEvent());

		// When
		consumerService.getMessagesMedia(List.of(message));

		// Then
		then(processingService).should().handleMessagesMedia(List.of(givenDigitalMediaEvent()));
	}

}
