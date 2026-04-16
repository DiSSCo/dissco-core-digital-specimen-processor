package eu.dissco.core.digitalspecimenprocessor.service.rabbitmqconsumer;

import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MAPPER;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalMediaTombstoneEvent;
import static org.mockito.BDDMockito.then;

import eu.dissco.core.digitalspecimenprocessor.service.preprocessing.ErPreprocessingService;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ErRabbitMqConsumerServiceTest {

	@Mock
	private ErPreprocessingService processingService;

	private ErRabbitMqConsumerService consumerService;

	@BeforeEach
	void setup() {
		consumerService = new ErRabbitMqConsumerService(MAPPER, processingService);
	}

	@Test
	void testGetMessagesDigitalMediaRelationshipTombstone() {
		// Given
		var message = MAPPER.writeValueAsString(givenDigitalMediaTombstoneEvent());

		// When
		consumerService.getMessagesDigitalMediaRelationshipTombstone(List.of(message));

		// Then
		then(processingService).should()
			.handleMessagesMediaRelationshipTombstone(List.of(givenDigitalMediaTombstoneEvent()));
	}

}
