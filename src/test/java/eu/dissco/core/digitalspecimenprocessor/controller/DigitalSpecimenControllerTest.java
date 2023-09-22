package eu.dissco.core.digitalspecimenprocessor.controller;

import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.CREATED;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.HANDLE;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimen;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.BDDMockito.given;

import eu.dissco.core.digitalspecimenprocessor.domain.DigitalSpecimenRecord;
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
class DigitalSpecimenControllerTest {

  @Mock
  private ProcessingService processingService;

  private DigitalSpecimenController controller;

  @BeforeEach
  void setup() {
    controller = new DigitalSpecimenController(processingService);
  }

  @Test
  void testDigitalSpecimenCreation() throws NoChangesFoundException {
    // Given
    var digitalSpecimenEvent = TestUtils.givenDigitalSpecimenEvent(true);
    given(processingService.handleMessages(
        List.of(digitalSpecimenEvent))).willReturn(
        List.of(new DigitalSpecimenRecord(HANDLE, 0, 1, CREATED, givenDigitalSpecimen())));

    // When
    var result = controller.upsertDigitalSpecimen(digitalSpecimenEvent);

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

}
