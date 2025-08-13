package eu.dissco.core.digitalspecimenprocessor.service;

import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.HANDLE;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MAS;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MEDIA_MAS;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.PHYSICAL_SPECIMEN_ID;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalMediaEvent;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalMediaRecord;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalMediaWrapper;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenEvent;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenRecord;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenWrapper;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenMasJobRequestMedia;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenMasJobRequestSpecimen;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.doThrow;

import com.fasterxml.jackson.core.JsonProcessingException;
import eu.dissco.core.digitalspecimenprocessor.domain.SpecimenProcessResult;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaEvent;
import eu.dissco.core.digitalspecimenprocessor.domain.media.MediaProcessResult;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenEvent;
import eu.dissco.core.digitalspecimenprocessor.property.ApplicationProperties;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
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

  @Test
  void testScheduleMasSpecimen() throws Exception {
    // Given

    // When
    masSchedulerService.scheduleMasSpecimenFromEvent(
        Set.of(givenDigitalSpecimenEvent()),
        List.of(givenDigitalSpecimenRecord()),
        new SpecimenProcessResult(
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
        new SpecimenProcessResult(List.of(givenDigitalSpecimenRecord()), List.of(), List.of(),
            Map.of()));

    // Then
    then(publisherService).shouldHaveNoInteractions();
  }

  @Test
  void testScheduleMaSpecimenNoNewSpecimenForced() throws Exception {
    // Given
    var event = new DigitalSpecimenEvent(
        List.of(MAS),
        givenDigitalSpecimenWrapper(),
        List.of(),
        true);

    // When
    masSchedulerService.scheduleMasSpecimenFromEvent(
        Set.of(event),
        List.of(givenDigitalSpecimenRecord()),
        new SpecimenProcessResult(List.of(givenDigitalSpecimenRecord()), List.of(), List.of(),
            Map.of()));

    // Then
    then(publisherService).should().publishMasJobRequest(givenMasJobRequestSpecimen());
  }

  @Test
  void testScheduleMaSpecimenNoMas() {
    // Given
    var event = new DigitalSpecimenEvent(
        List.of(),
        givenDigitalSpecimenWrapper(),
        List.of(),
        false);

    // When
    masSchedulerService.scheduleMasSpecimenFromEvent(
        Set.of(event),
        List.of(givenDigitalSpecimenRecord()),
        new SpecimenProcessResult(List.of(givenDigitalSpecimenRecord()), List.of(), List.of(),
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
        new SpecimenProcessResult(
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
        new MediaProcessResult(List.of(), List.of(), List.of(givenDigitalMediaEvent())));

    // Then
    then(publisherService).should().publishMasJobRequest(givenMasJobRequestMedia());
  }

  @Test
  void testScheduleMasMediaNoNewMedia() {
    // Given

    // When
    masSchedulerService.scheduleMasMediaFromEvent(Set.of(givenDigitalMediaEvent()),
        List.of(givenDigitalMediaRecord()),
        new MediaProcessResult(List.of(givenDigitalMediaRecord()), List.of(), List.of()));

    // Then
    then(publisherService).shouldHaveNoInteractions();
  }

  @Test
  void testScheduleMasMediaNoNewMediaForced() throws Exception {
    // Given
    var event = new DigitalMediaEvent(
        List.of(MEDIA_MAS),
        givenDigitalMediaWrapper(), true);

    // When
    masSchedulerService.scheduleMasMediaFromEvent(Set.of(event), List.of(givenDigitalMediaRecord()),
        new MediaProcessResult(List.of(givenDigitalMediaRecord()), List.of(), List.of()));

    // Then
    then(publisherService).should().publishMasJobRequest(givenMasJobRequestMedia());
  }

  @Test
  void testScheduleMasMediaNoMas() {
    // Given
    var event = new DigitalMediaEvent(
        List.of(),
        givenDigitalMediaWrapper(), false);

    // When
    masSchedulerService.scheduleMasMediaFromEvent(Set.of(event), List.of(givenDigitalMediaRecord()),
        new MediaProcessResult(List.of(givenDigitalMediaRecord()), List.of(), List.of()));

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
            new MediaProcessResult(List.of(), List.of(), List.of(givenDigitalMediaEvent()))));
  }
}
