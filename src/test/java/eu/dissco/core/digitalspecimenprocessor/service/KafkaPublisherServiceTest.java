package eu.dissco.core.digitalspecimenprocessor.service;

import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.AAS;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MAPPER;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenEvent;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenRecord;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenUnequalDigitalSpecimenRecord;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.times;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

@ExtendWith(MockitoExtension.class)
class KafkaPublisherServiceTest {

  @Mock
  private KafkaTemplate<String, String> kafkaTemplate;

  private KafkaPublisherService service;


  @BeforeEach
  void setup() {
    service = new KafkaPublisherService(MAPPER, kafkaTemplate);
  }

  @Test
  void testPublishCreateEvent() throws JsonProcessingException {
    // Given

    // When
    service.publishCreateEvent(givenDigitalSpecimenRecord());

    // Then
    then(kafkaTemplate).should().send(eq("createUpdateDeleteTopic"), anyString());
  }

  @Test
  void testPublishAnnotationRequestEvent() throws JsonProcessingException {
    // Given

    // When
    service.publishAnnotationRequestEvent(AAS, givenDigitalSpecimenRecord());

    // Then
    then(kafkaTemplate).should().send(eq(AAS), anyString());
  }

  @Test
  void testPublishUpdateEvent() throws JsonProcessingException {
    // Given

    // When
    service.publishUpdateEvent(givenDigitalSpecimenRecord(2), givenUnequalDigitalSpecimenRecord());

    // Then
    then(kafkaTemplate).should().send(eq("createUpdateDeleteTopic"), anyString());
  }

  @Test
  void testRepublishEvent() throws JsonProcessingException {
    // Given

    // When
    service.republishEvent(givenDigitalSpecimenEvent());

    // Then
    then(kafkaTemplate).should()
        .send("digital-specimen", MAPPER.writeValueAsString(givenDigitalSpecimenEvent()));
  }

  @Test
  void testDeadLetterEvent() throws JsonProcessingException {
    // Given

    // When
    service.deadLetterEvent(givenDigitalSpecimenEvent());

    // Then
    then(kafkaTemplate).should()
        .send("digital-specimen-dlq", MAPPER.writeValueAsString(givenDigitalSpecimenEvent()));
  }

  @Test
  void testDeadLetterEventList() throws JsonProcessingException {
    // Given
    var expectedEvents = List.of(givenDigitalSpecimenEvent(), givenDigitalSpecimenEvent());

    // When
    service.deadLetterEvent(expectedEvents);

    // Then
    then(kafkaTemplate).should(times(2))
        .send("digital-specimen-dlq", MAPPER.writeValueAsString(expectedEvents.get(0)));
  }

  @Test
  void testDeadLetterRaw() throws JsonProcessingException {
    // Given
    var rawEvent = MAPPER.writeValueAsString(givenDigitalSpecimenEvent());

    // When
    service.deadLetterRaw(rawEvent);

    // Then
    then(kafkaTemplate).should().send("digital-specimen-dlq", rawEvent);
  }
}
