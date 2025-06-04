package eu.dissco.core.digitalspecimenprocessor.controller;

import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.CREATED;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.HANDLE;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalMediaEvent;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalMediaRecord;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenWrapper;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.BDDMockito.given;

import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenRecord;
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
        List.of(new DigitalSpecimenRecord(HANDLE, 0, 1, CREATED, givenDigitalSpecimenWrapper())));

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
        List.of(digitalmediaEvent))).willReturn(List.of(givenDigitalMediaRecord()));

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
        List.of(digitalSpecimenEvent))).willReturn(List.of());

    // When / Then
    assertThrows(NoChangesFoundException.class,
        () -> controller.upsertDigitalSpecimen(digitalSpecimenEvent));
  }

  @Test
  void testNoChangesMedia() {
    // Given
    given(processingService.handleMessagesMedia(
        List.of(givenDigitalMediaEvent()))).willReturn(List.of());

    // When / Then
    assertThrows(NoChangesFoundException.class,
        () -> controller.upsertDigitalMedia(givenDigitalMediaEvent()));
  }

}
