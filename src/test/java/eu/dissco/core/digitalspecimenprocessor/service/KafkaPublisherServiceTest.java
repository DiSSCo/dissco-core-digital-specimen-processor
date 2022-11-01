package eu.dissco.core.digitalspecimenprocessor.service;

import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.AAS;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MAPPER;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenRecord;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenUnequalDigitalSpecimenRecord;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.then;

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
  void testPublishCreateEvent() {
    // Given

    // When
    service.publishCreateEvent(givenDigitalSpecimenRecord());

    // Then
    then(kafkaTemplate).should().send(eq("createUpdateDeleteTopic"), anyString());
  }

  @Test
  void testPublishAnnotationRequestEvent() {
    // Given

    // When
    service.publishAnnotationRequestEvent(AAS, givenDigitalSpecimenRecord());

    // Then
    then(kafkaTemplate).should().send(eq(AAS), anyString());
  }

  @Test
  void testPublishUpdateEvent() {
    // Given

    // When
    service.publishUpdateEvent(givenDigitalSpecimenRecord(2), givenUnequalDigitalSpecimenRecord());

    // Then
    then(kafkaTemplate).should().send(eq("createUpdateDeleteTopic"), anyString());
  }

}
