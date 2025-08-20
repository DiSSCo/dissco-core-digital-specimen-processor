package eu.dissco.core.digitalspecimenprocessor.service;

import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.APP_HANDLE;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.CREATED;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.HANDLE;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MAPPER;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MAS;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MEDIA_MAS;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MEDIA_URL;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MEDIA_URL_ALT;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.SECOND_HANDLE;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalMedia;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalMediaRecord;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenRecord;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenWrapper;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.doThrow;

import com.fasterxml.jackson.core.JsonProcessingException;
import eu.dissco.core.digitalspecimenprocessor.database.jooq.enums.MjrTargetType;
import eu.dissco.core.digitalspecimenprocessor.domain.mas.MasJobRequest;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaRecord;
import eu.dissco.core.digitalspecimenprocessor.domain.media.MediaProcessResult;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenRecord;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.SpecimenProcessResult;
import eu.dissco.core.digitalspecimenprocessor.property.ApplicationProperties;
import java.util.List;
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
  void testPublishSpecimenForced() throws Exception {
    // Given
    var forcedRecord = new DigitalSpecimenRecord(
        HANDLE,
        1,
        1,
        CREATED,
        givenDigitalSpecimenWrapper(),
        Set.of(MAS),
        true
    );
    var specimenProcessResult = new SpecimenProcessResult(
        List.of(forcedRecord), List.of(), List.of(givenDigitalSpecimenRecord(SECOND_HANDLE)));

    // When
    masSchedulerService.scheduleMasForSpecimen(specimenProcessResult);

    // Then
    then(publisherService).should().publishMasJobRequest(new MasJobRequest(
        MAS, HANDLE, false, APP_HANDLE, MjrTargetType.DIGITAL_SPECIMEN
    ));
    then(publisherService).should().publishMasJobRequest(new MasJobRequest(
        MAS, SECOND_HANDLE, false, APP_HANDLE, MjrTargetType.DIGITAL_SPECIMEN
    ));
  }

  @Test
  void testPublishSpecimenNotForced() throws Exception {
    // Given
    var specimenProcessResult = new SpecimenProcessResult(
        List.of(givenDigitalSpecimenRecord()), List.of(),
        List.of(givenDigitalSpecimenRecord(SECOND_HANDLE)));

    // When
    masSchedulerService.scheduleMasForSpecimen(specimenProcessResult);

    // Then
    then(publisherService).should().publishMasJobRequest(new MasJobRequest(
        MAS, SECOND_HANDLE, false, APP_HANDLE, MjrTargetType.DIGITAL_SPECIMEN
    ));
    then(publisherService).shouldHaveNoMoreInteractions();
  }

  @Test
  void testPublishMediaNotForced() throws Exception {
    // Given
    var mediaProcessResult = new MediaProcessResult(
        List.of(givenDigitalMediaRecord()), List.of(), List.of(givenDigitalMediaRecord(
        SECOND_HANDLE, MEDIA_URL_ALT, 1
    )));

    // When
    masSchedulerService.scheduleMasForMedia(mediaProcessResult);

    // Then
    then(publisherService).should().publishMasJobRequest(new MasJobRequest(
        MEDIA_MAS, SECOND_HANDLE, false, APP_HANDLE, MjrTargetType.MEDIA_OBJECT
    ));
    then(publisherService).shouldHaveNoMoreInteractions();
  }

  @Test
  void testPublishMediaForced() throws Exception {
    // Given
    var forcedRecord = new DigitalMediaRecord(
        HANDLE, MEDIA_URL, 1, CREATED, Set.of(MEDIA_MAS),
        givenDigitalMedia(MEDIA_URL),
        MAPPER.createObjectNode(), true);

    var mediaProcessResult = new MediaProcessResult(
        List.of(forcedRecord), List.of(), List.of(givenDigitalMediaRecord(
        SECOND_HANDLE, MEDIA_URL, 1
    )));

    // When
    masSchedulerService.scheduleMasForMedia(mediaProcessResult);

    // Then
    then(publisherService).should().publishMasJobRequest(new MasJobRequest(
        MEDIA_MAS, SECOND_HANDLE, false, APP_HANDLE, MjrTargetType.MEDIA_OBJECT
    ));
    then(publisherService).should().publishMasJobRequest(new MasJobRequest(
        MEDIA_MAS, HANDLE, false, APP_HANDLE, MjrTargetType.MEDIA_OBJECT
    ));
  }

  @Test
  void testPublishSpecimenPublishingFails() throws Exception {
    // Given
    var specimenProcessResult = new SpecimenProcessResult(
        List.of(givenDigitalSpecimenRecord()), List.of(),
        List.of(givenDigitalSpecimenRecord(SECOND_HANDLE)));
    doThrow(JsonProcessingException.class).when(publisherService).publishMasJobRequest(any());

    // When / then
    assertDoesNotThrow(() -> masSchedulerService.scheduleMasForSpecimen(specimenProcessResult));
  }
}
