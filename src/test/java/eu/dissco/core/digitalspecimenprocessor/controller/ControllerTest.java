package eu.dissco.core.digitalspecimenprocessor.controller;

import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalMediaEvent;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalMediaRecord;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenRecord;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.BDDMockito.given;

import eu.dissco.core.digitalspecimenprocessor.domain.media.MediaProcessResult;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.SpecimenProcessResult;
import eu.dissco.core.digitalspecimenprocessor.exception.NoChangesFoundException;
import eu.dissco.core.digitalspecimenprocessor.service.ProcessingService;
import eu.dissco.core.digitalspecimenprocessor.utils.TestUtils;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;

@ExtendWith(MockitoExtension.class)
class ControllerTest {

  @Mock
  private ProcessingService processingService;

  private Controller controller;

  @BeforeEach
  void setup() {
    controller = new Controller(processingService);
  }

  @Test
  void testDigitalSpecimenCreation() throws NoChangesFoundException {
    // Given
    var digitalSpecimenEvent = TestUtils.givenDigitalSpecimenEvent(true);
    given(processingService.handleMessages(
        List.of(digitalSpecimenEvent))).willReturn(
        new SpecimenProcessResult(List.of(), List.of(), List.of(givenDigitalSpecimenRecord())));

    // When
    var result = controller.upsertDigitalSpecimen(digitalSpecimenEvent);

    // Then
    assertThat(result.getStatusCode()).isEqualTo(HttpStatus.CREATED);
  }

  @Test
  void testDigitalMediaCreation() throws NoChangesFoundException {
    // Given
    var digitalmediaEvent = TestUtils.givenDigitalMediaEvent();
    given(processingService.handleMessagesMedia(
        List.of(digitalmediaEvent))).willReturn(
        new MediaProcessResult(List.of(), List.of(), List.of(givenDigitalMediaRecord())));

    // When
    var result = controller.upsertDigitalMedia(digitalmediaEvent);

    // Then
    assertThat(result.getStatusCode()).isEqualTo(HttpStatus.CREATED);
  }

  @Test
  void testNoChanges() {
    // Given
    var digitalSpecimenEvent = TestUtils.givenDigitalSpecimenEvent(true);
    given(processingService.handleMessages(
        List.of(digitalSpecimenEvent))).willReturn(
        new SpecimenProcessResult(List.of(), List.of(), List.of()));

    // When / Then
    assertThrows(NoChangesFoundException.class,
        () -> controller.upsertDigitalSpecimen(digitalSpecimenEvent));
  }

  @Test
  void testNoChangesMedia() {
    // Given
    given(processingService.handleMessagesMedia(
        List.of(givenDigitalMediaEvent()))).willReturn(
        new MediaProcessResult(List.of(), List.of(), List.of()));

    // When / Then
    assertThrows(NoChangesFoundException.class,
        () -> controller.upsertDigitalMedia(givenDigitalMediaEvent()));
  }

}
