package eu.dissco.core.digitalspecimenprocessor.service;

import eu.dissco.core.digitalspecimenprocessor.property.ApplicationProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class MasSchedulerTest {

  private MasSchedulerService masSchedulerService;

  @Mock
  private RabbitMqPublisherService publisherService;

  @BeforeEach
  void setup() {
    masSchedulerService = new MasSchedulerService(
        publisherService, new ApplicationProperties()
    );
  }

  /*
  @Test
  void testScheduleMasSpecimen() throws Exception {
    // Given

    // When
    masSchedulerService.scheduleMasSpecimenFromEvent(
        Set.of(givenDigitalSpecimenEvent()),
        List.of(givenDigitalSpecimenRecord()),
        new SpecimenPreprocessResult(
            List.of(),
            List.of(),
            List.of(givenDigitalSpecimenEvent()),
            Map.of(PHYSICAL_SPECIMEN_ID, HANDLE)
        ));

    // Then
    then(publisherService).should().publishMasJobRequest(givenMasJobRequestSpecimen());
  }


  @Test
  void testScheduleMaSpecimenNoNewSpecimen() {
    // Given

    // When
    masSchedulerService.scheduleMasSpecimenFromEvent(
        Set.of(givenDigitalSpecimenEvent()),
        List.of(givenDigitalSpecimenRecord()),
        new SpecimenPreprocessResult(List.of(givenDigitalSpecimenRecord()), List.of(), List.of(),
            Map.of()));

    // Then
    then(publisherService).shouldHaveNoInteractions();
  }

  @Test
  void testScheduleMaSpecimenNoNewSpecimenForced() throws Exception {
    // Given
    var event = new DigitalSpecimenEvent(
        Set.of(MAS),
        givenDigitalSpecimenWrapper(),
        List.of(),
        true);

    // When
    masSchedulerService.scheduleMasSpecimenFromEvent(
        Set.of(event),
        List.of(givenDigitalSpecimenRecord()),
        new SpecimenPreprocessResult(List.of(givenDigitalSpecimenRecord()), List.of(), List.of(),
            Map.of()));

    // Then
    then(publisherService).should().publishMasJobRequest(givenMasJobRequestSpecimen());
  }

  @Test
  void testScheduleMaSpecimenNoMas() {
    // Given
    var event = new DigitalSpecimenEvent(
        Set.of(),
        givenDigitalSpecimenWrapper(),
        List.of(),
        false);

    // When
    masSchedulerService.scheduleMasSpecimenFromEvent(
        Set.of(event),
        List.of(givenDigitalSpecimenRecord()),
        new SpecimenPreprocessResult(List.of(givenDigitalSpecimenRecord()), List.of(), List.of(),
            Map.of()));

    // Then
    then(publisherService).shouldHaveNoInteractions();
  }

  @Test
  void testScheduleMasSpecimenJpe() throws Exception {
    // Given
    doThrow(JsonProcessingException.class).when(publisherService)
        .publishMasJobRequest(givenMasJobRequestSpecimen());

    // When / Then
    assertDoesNotThrow(() -> masSchedulerService.scheduleMasSpecimenFromEvent(
        Set.of(givenDigitalSpecimenEvent()),
        List.of(givenDigitalSpecimenRecord()),
        new SpecimenPreprocessResult(
            List.of(),
            List.of(),
            List.of(givenDigitalSpecimenEvent()),
            Map.of(PHYSICAL_SPECIMEN_ID, HANDLE)
        )));
  }

  @Test
  void testScheduleMasMedia() throws Exception {
    // Given

    // When
    masSchedulerService.scheduleMasMediaFromEvent(Set.of(givenDigitalMediaEvent()),
        List.of(givenDigitalMediaRecord()),
        new MediaPreprocessResult(List.of(), List.of(), List.of(givenDigitalMediaEvent())));

    // Then
    then(publisherService).should().publishMasJobRequest(givenMasJobRequestMedia());
  }

  @Test
  void testScheduleMasMediaNoNewMedia() {
    // Given

    // When
    masSchedulerService.scheduleMasMediaFromEvent(Set.of(givenDigitalMediaEvent()),
        List.of(givenDigitalMediaRecord()),
        new MediaPreprocessResult(List.of(givenDigitalMediaRecord()), List.of(), List.of()));

    // Then
    then(publisherService).shouldHaveNoInteractions();
  }

  @Test
  void testScheduleMasMediaNoNewMediaForced() throws Exception {
    // Given
    var event = new DigitalMediaEvent(
        Set.of(MEDIA_MAS),
        givenDigitalMediaWrapper(), true);

    // When
    masSchedulerService.scheduleMasMediaFromEvent(Set.of(event), List.of(givenDigitalMediaRecord()),
        new MediaPreprocessResult(List.of(givenDigitalMediaRecord()), List.of(), List.of()));

    // Then
    then(publisherService).should().publishMasJobRequest(givenMasJobRequestMedia());
  }

  @Test
  void testScheduleMasMediaNoMas() {
    // Given
    var event = new DigitalMediaEvent(
        Set.of(),
        givenDigitalMediaWrapper(), false);

    // When
    masSchedulerService.scheduleMasMediaFromEvent(Set.of(event), List.of(givenDigitalMediaRecord()),
        new MediaPreprocessResult(List.of(givenDigitalMediaRecord()), List.of(), List.of()));

    // Then
    then(publisherService).shouldHaveNoInteractions();
  }

  @Test
  void testScheduleMasMediaJpe() throws Exception {
    // Given
    doThrow(JsonProcessingException.class).when(publisherService)
        .publishMasJobRequest(givenMasJobRequestMedia());

    // When / Then
    assertDoesNotThrow(
        () -> masSchedulerService.scheduleMasMediaFromEvent(Set.of(givenDigitalMediaEvent()),
            List.of(givenDigitalMediaRecord()),
            new MediaPreprocessResult(List.of(), List.of(), List.of(givenDigitalMediaEvent()))));
  }

   */
}
