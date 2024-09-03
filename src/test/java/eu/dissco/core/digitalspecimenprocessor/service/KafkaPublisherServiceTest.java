package eu.dissco.core.digitalspecimenprocessor.service;

import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MAPPER;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MAS;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenAutoAcceptedAnnotation;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalMediaEventWithRelationship;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenEvent;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenRecord;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenJsonPatch;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenNewAcceptedAnnotation;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.then;

import com.fasterxml.jackson.core.JsonProcessingException;
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
  @Mock
  private ProvenanceService provenanceService;

  private KafkaPublisherService service;


  @BeforeEach
  void setup() {
    service = new KafkaPublisherService(MAPPER, kafkaTemplate, provenanceService);
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
    service.publishAnnotationRequestEvent(MAS, givenDigitalSpecimenRecord());

    // Then
    then(kafkaTemplate).should().send(eq(MAS), anyString());
  }

  @Test
  void testPublishUpdateEvent() throws JsonProcessingException {
    // Given

    // When
    service.publishUpdateEvent(givenDigitalSpecimenRecord(2), givenJsonPatch());

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
  void testDeadLetterRaw() throws JsonProcessingException {
    // Given
    var rawEvent = MAPPER.writeValueAsString(givenDigitalSpecimenEvent());

    // When
    service.deadLetterRaw(rawEvent);

    // Then
    then(kafkaTemplate).should().send("digital-specimen-dlq", rawEvent);
  }

  @Test
  void testPublishDigitalMediaObjectEvent() throws JsonProcessingException {
    // Given
    var digitalMediaObjectEvent = givenDigitalMediaEventWithRelationship();

    // When
    service.publishDigitalMediaObject(digitalMediaObjectEvent);

    // Then
    then(kafkaTemplate).should()
        .send("digital-media", MAPPER.writeValueAsString(digitalMediaObjectEvent));
  }

  @Test
  void testPublishAcceptedAnnotation() throws JsonProcessingException {
    // Given
    var newAcceptedAnnotation = givenAutoAcceptedAnnotation(givenNewAcceptedAnnotation());

    // When
    service.publishAcceptedAnnotation(newAcceptedAnnotation);

    // Then
    then(kafkaTemplate).should()
        .send("annotation-processing", MAPPER.writeValueAsString(newAcceptedAnnotation));
  }
}
