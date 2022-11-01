package eu.dissco.core.digitalspecimenprocessor.service;

import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.AAS;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.CREATED;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.HANDLE;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.PHYSICAL_SPECIMEN_ID;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimen;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenEvent;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenRecord;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenUnequalDigitalSpecimenRecord;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.mockStatic;

import co.elastic.clients.elasticsearch.core.BulkResponse;
import eu.dissco.core.digitalspecimenprocessor.domain.DigitalSpecimenRecord;
import eu.dissco.core.digitalspecimenprocessor.repository.DigitalSpecimenRepository;
import eu.dissco.core.digitalspecimenprocessor.repository.ElasticSearchRepository;
import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Set;
import javax.xml.transform.TransformerException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ProcessingServiceTest {

  @Mock
  private DigitalSpecimenRepository repository;
  @Mock
  private HandleService handleService;
  @Mock
  private ElasticSearchRepository elasticRepository;
  @Mock
  private KafkaPublisherService kafkaService;
  @Mock
  private BulkResponse bulkResponse;
  private MockedStatic<Instant> mockedStatic;

  private ProcessingService service;

  @BeforeEach
  void setup() {
    service = new ProcessingService(repository, handleService, elasticRepository, kafkaService);
    Clock clock = Clock.fixed(CREATED, ZoneOffset.UTC);
    Instant instant = Instant.now(clock);
    mockedStatic = mockStatic(Instant.class);
    mockedStatic.when(Instant::now).thenReturn(instant);
  }

  @AfterEach
  void destroy() {
    mockedStatic.close();
  }

  @Test
  void testEqualSpecimen() {
    // Given
    given(repository.getDigitalSpecimens(List.of(PHYSICAL_SPECIMEN_ID))).willReturn(
        List.of(givenDigitalSpecimenRecord()));

    // When
    List<DigitalSpecimenRecord> result = service.handleMessages(List.of(givenDigitalSpecimenEvent()));

    // Then
    then(repository).should().updateLastChecked(List.of(HANDLE));
    assertThat(result).isEmpty();
  }

  @Test
  void testUnequalSpecimen() throws IOException {
    // Given
    var expected = Set.of(givenDigitalSpecimenRecord(2));
    given(repository.getDigitalSpecimens(List.of(PHYSICAL_SPECIMEN_ID))).willReturn(
        List.of(givenUnequalDigitalSpecimenRecord()));
    given(bulkResponse.errors()).willReturn(false);
    given(
        elasticRepository.indexDigitalSpecimen(expected)).willReturn(
        bulkResponse);

    // When
    var result = service.handleMessages(List.of(givenDigitalSpecimenEvent()));

    // Then
    then(repository).should().createDigitalSpecimenRecord(expected);
    then(kafkaService).should()
        .publishUpdateEvent(givenDigitalSpecimenRecord(2), givenUnequalDigitalSpecimenRecord());
    assertThat(result).isEqualTo(List.of(givenDigitalSpecimenRecord(2)));
  }

  @Test
  void testNewSpecimen() throws TransformerException, IOException {
    // Given
    given(repository.getDigitalSpecimens(List.of(PHYSICAL_SPECIMEN_ID))).willReturn(List.of());
    given(handleService.createNewHandle(givenDigitalSpecimen())).willReturn(HANDLE);
    given(bulkResponse.errors()).willReturn(false);
    given(
        elasticRepository.indexDigitalSpecimen(Set.of(givenDigitalSpecimenRecord()))).willReturn(
        bulkResponse);

    // When
    var result = service.handleMessages(List.of(givenDigitalSpecimenEvent()));

    // Then
    then(repository).should().createDigitalSpecimenRecord(Set.of(givenDigitalSpecimenRecord()));
    then(kafkaService).should().publishCreateEvent(givenDigitalSpecimenRecord());
    then(kafkaService).should().publishAnnotationRequestEvent(AAS, givenDigitalSpecimenRecord());
    assertThat(result).isEqualTo(List.of(givenDigitalSpecimenRecord()));
  }

  @Test
  void testNewSpecimenError() throws TransformerException {
    // Given
    given(repository.getDigitalSpecimens(List.of(PHYSICAL_SPECIMEN_ID))).willReturn(List.of());
    given(handleService.createNewHandle(givenDigitalSpecimen())).willThrow(
        TransformerException.class);

    // When
    var result = service.handleMessages(List.of(givenDigitalSpecimenEvent()));

    // Then
    then(repository).shouldHaveNoMoreInteractions();
    then(elasticRepository).shouldHaveNoInteractions();
    assertThat(result).isEmpty();
  }

}
