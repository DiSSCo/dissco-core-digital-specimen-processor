package eu.dissco.core.digitalspecimenprocessor.service;

import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.HANDLE;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.HANDLE_PREFIX;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.PHYSICAL_SPECIMEN_ID;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.SECOND_HANDLE;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.SOURCE_SYSTEM_ID;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.THIRD_HANDLE;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalMediaEvent;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalMediaRecord;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenEvent;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenRecord;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenMasJobRequestMedia;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenMasJobRequestSpecimen;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;

import eu.dissco.core.digitalspecimenprocessor.domain.SpecimenProcessResult;
import eu.dissco.core.digitalspecimenprocessor.domain.mas.SourceSystemMass;
import eu.dissco.core.digitalspecimenprocessor.domain.media.MediaProcessResult;
import eu.dissco.core.digitalspecimenprocessor.property.ApplicationProperties;
import eu.dissco.core.digitalspecimenprocessor.repository.SourceSystemRepository;
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
  @Mock
  private SourceSystemRepository sourceSystemRepository;

  @BeforeEach
  void setup() {
    masSchedulerService = new MasSchedulerService(
        publisherService, sourceSystemRepository, new ApplicationProperties()
    );
  }

  @Test
  void testScheduleMasSpecimen() throws Exception {
    // Given
    given(sourceSystemRepository.getSourceSystemMass(
        Set.of(SOURCE_SYSTEM_ID.replace(HANDLE_PREFIX, ""))))
        .willReturn(givenSourceSystemResponse());

    // When
    masSchedulerService.scheduleMasSpecimen(List.of(givenDigitalSpecimenRecord()),
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
    masSchedulerService.scheduleMasSpecimen(List.of(givenDigitalSpecimenRecord()),
        new SpecimenProcessResult(List.of(givenDigitalSpecimenRecord()), List.of(), List.of(),
            Map.of()));

    // Then
    then(publisherService).shouldHaveNoInteractions();
  }

  @Test
  void testScheduleMasMedia() throws Exception {
    // Given
    given(sourceSystemRepository.getSourceSystemMass(
        Set.of(SOURCE_SYSTEM_ID.replace(HANDLE_PREFIX, ""))))
        .willReturn(givenSourceSystemResponse());
    var media = givenDigitalMediaRecord();
    media.attributes().setOdsSourceSystemID(SOURCE_SYSTEM_ID);

    // When
    masSchedulerService.scheduleMasMedia(List.of(media),
        new MediaProcessResult(List.of(), List.of(), List.of(givenDigitalMediaEvent())));

    // Then
    then(publisherService).should().publishMasJobRequest(givenMasJobRequestMedia());
  }

  @Test
  void testScheduleMaSpecimenNoNewMedia() {
    // Given

    // When
    masSchedulerService.scheduleMasMedia(List.of(givenDigitalMediaRecord()),
        new MediaProcessResult(List.of(givenDigitalMediaRecord()), List.of(), List.of()));

    // Then
    then(publisherService).shouldHaveNoInteractions();
  }

  private static Map<String, SourceSystemMass> givenSourceSystemResponse() {
    return Map.of(SOURCE_SYSTEM_ID.replace(HANDLE_PREFIX, ""),
        new SourceSystemMass(List.of(SECOND_HANDLE), List.of(THIRD_HANDLE))
    );
  }

}
