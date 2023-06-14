package eu.dissco.core.digitalspecimenprocessor.service;

import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;

import co.elastic.clients.elasticsearch._types.ErrorCause;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import com.fasterxml.jackson.core.JsonProcessingException;
import eu.dissco.core.digitalspecimenprocessor.component.FdoRecordBuilder;
import eu.dissco.core.digitalspecimenprocessor.component.HandleComponent;
import eu.dissco.core.digitalspecimenprocessor.domain.DigitalSpecimen;
import eu.dissco.core.digitalspecimenprocessor.domain.DigitalSpecimenEvent;
import eu.dissco.core.digitalspecimenprocessor.domain.DigitalSpecimenRecord;
import eu.dissco.core.digitalspecimenprocessor.domain.UpdatedDigitalSpecimenTuple;
import eu.dissco.core.digitalspecimenprocessor.exception.DisscoRepositoryException;
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
  private FdoRecordBuilder fdoRecordBuilder;
  @Mock
  private ElasticSearchRepository elasticRepository;
  @Mock
  private KafkaPublisherService kafkaService;
  @Mock
  private BulkResponse bulkResponse;
  @Mock
  private MidsService midsService;

  @Mock
  private HandleComponent handleComponent;

  private MockedStatic<Instant> mockedInstant;
  private MockedStatic<Clock> mockedClock;
  private ProcessingService service;
  @BeforeEach
  void setup() {
    service = new ProcessingService(repository, fdoRecordBuilder, elasticRepository, kafkaService,
        midsService);
    Clock clock = Clock.fixed(CREATED, ZoneOffset.UTC);
    Instant instant = Instant.now(clock);
    mockedInstant = mockStatic(Instant.class);
    mockedInstant.when(Instant::now).thenReturn(instant);
    mockedClock = mockStatic(Clock.class);
    mockedClock.when(Clock::systemUTC).thenReturn(clock);
  }

  @AfterEach
  void destroy() {
    mockedInstant.close();
    mockedClock.close();
  }

  @Test
  void testEqualSpecimen() throws DisscoRepositoryException {
    // Given
    given(repository.getDigitalSpecimens(List.of(PHYSICAL_SPECIMEN_ID))).willReturn(
        List.of(givenDigitalSpecimenRecord()));

    // When
    List<DigitalSpecimenRecord> result = service.handleMessages(
        List.of(givenDigitalSpecimenEvent()));

    // Then
    then(repository).should().updateLastChecked(List.of(HANDLE));
    assertThat(result).isEmpty();
  }

  @Test
  void testUnequalSpecimen() throws IOException, DisscoRepositoryException {
    // Given
    var expected = List.of(givenDigitalSpecimenRecord(2));
    given(repository.getDigitalSpecimens(List.of(PHYSICAL_SPECIMEN_ID))).willReturn(
        List.of(givenUnequalDigitalSpecimenRecord()));
    given(bulkResponse.errors()).willReturn(false);
    given(
        elasticRepository.indexDigitalSpecimen(expected)).willReturn(
        bulkResponse);
    given(midsService.calculateMids(givenDigitalSpecimen())).willReturn(1);

    // When
    var result = service.handleMessages(List.of(givenDigitalSpecimenEvent()));

    // Then
    then(fdoRecordBuilder).should()
        .updateHandles(List.of(new UpdatedDigitalSpecimenTuple(givenUnequalDigitalSpecimenRecord(),
            givenDigitalSpecimenEvent())));
    then(repository).should().createDigitalSpecimenRecord(expected);
    then(kafkaService).should()
        .publishUpdateEvent(givenDigitalSpecimenRecord(2), givenUnequalDigitalSpecimenRecord());
    assertThat(result).isEqualTo(List.of(givenDigitalSpecimenRecord(2)));
  }

  @Test
  void testNewSpecimen() throws Exception {
    // Given
    given(repository.getDigitalSpecimens(List.of(PHYSICAL_SPECIMEN_ID))).willReturn(List.of());
    given(fdoRecordBuilder.createNewHandle(givenDigitalSpecimen())).willReturn(HANDLE);
    given(bulkResponse.errors()).willReturn(false);
    given(
        elasticRepository.indexDigitalSpecimen(Set.of(givenDigitalSpecimenRecord()))).willReturn(
        bulkResponse);
    given(midsService.calculateMids(givenDigitalSpecimen())).willReturn(1);

    // When
    var result = service.handleMessages(List.of(givenDigitalSpecimenEvent()));

    // Then
    then(repository).should().createDigitalSpecimenRecord(Set.of(givenDigitalSpecimenRecord()));
    then(kafkaService).should().publishCreateEvent(givenDigitalSpecimenRecord());
    then(kafkaService).should().publishAnnotationRequestEvent(AAS, givenDigitalSpecimenRecord());
    assertThat(result).isEqualTo(List.of(givenDigitalSpecimenRecord()));
  }

  @Test
  void testDuplicateNewSpecimen()
      throws Exception {
    // Given
    var duplicateSpecimen = new DigitalSpecimenEvent(List.of(AAS),
        givenDigitalSpecimen(PHYSICAL_SPECIMEN_ID, ANOTHER_SPECIMEN_NAME, ANOTHER_ORGANISATION));
    given(repository.getDigitalSpecimens(List.of(PHYSICAL_SPECIMEN_ID))).willReturn(List.of());
    given(fdoRecordBuilder.createNewHandle(givenDigitalSpecimen())).willReturn(HANDLE);
    given(bulkResponse.errors()).willReturn(false);
    given(
        elasticRepository.indexDigitalSpecimen(Set.of(givenDigitalSpecimenRecord()))).willReturn(
        bulkResponse);
    given(midsService.calculateMids(givenDigitalSpecimen())).willReturn(1);

    // When
    var result = service.handleMessages(
        List.of(givenDigitalSpecimenEvent(), duplicateSpecimen));

    // Then
    then(repository).should().createDigitalSpecimenRecord(Set.of(givenDigitalSpecimenRecord()));
    then(kafkaService).should().publishCreateEvent(givenDigitalSpecimenRecord());
    then(kafkaService).should().publishAnnotationRequestEvent(AAS, givenDigitalSpecimenRecord());
    then(kafkaService).should().republishEvent(duplicateSpecimen);
    assertThat(result).isEqualTo(List.of(givenDigitalSpecimenRecord()));
  }

  @Test
  void testNewSpecimenIOException()
      throws Exception {
    // Given
    given(repository.getDigitalSpecimens(List.of(PHYSICAL_SPECIMEN_ID))).willReturn(List.of());
    given(fdoRecordBuilder.createNewHandle(givenDigitalSpecimen())).willReturn(HANDLE);
    given(
        elasticRepository.indexDigitalSpecimen(Set.of(givenDigitalSpecimenRecord()))).willThrow(
        IOException.class);
    given(midsService.calculateMids(givenDigitalSpecimen())).willReturn(1);

    // When
    var result = service.handleMessages(List.of(givenDigitalSpecimenEvent()));

    // Then
    then(repository).should().createDigitalSpecimenRecord(Set.of(givenDigitalSpecimenRecord()));
    then(repository).should().rollbackSpecimen(givenDigitalSpecimenRecord().id());
    then(fdoRecordBuilder).should().rollbackHandleCreation(givenDigitalSpecimenRecord());
    then(kafkaService).should().deadLetterEvent(givenDigitalSpecimenEvent());
    assertThat(result).isEmpty();
  }

  @Test
  void testNewSpecimenPartialElasticFailed()
      throws Exception {
    // Given
    var secondEvent = givenDigitalSpecimenEvent("Another Specimen");
    var secondSpecimen = givenDigitalSpecimenRecord(SECOND_HANDLE, "Another Specimen");
    var thirdEvent = givenDigitalSpecimenEvent("A third Specimen");
    var thirdSpecimen = givenDigitalSpecimenRecord(THIRD_HANDLE, "A third Specimen");
    given(repository.getDigitalSpecimens(anyList())).willReturn(List.of());
    given(fdoRecordBuilder.createNewHandle(any(DigitalSpecimen.class))).willReturn(THIRD_HANDLE)
        .willReturn(SECOND_HANDLE).willReturn(HANDLE);
    given(midsService.calculateMids(any(DigitalSpecimen.class))).willReturn(1);
    givenBulkResponse();
    given(elasticRepository.indexDigitalSpecimen(anySet())).willReturn(bulkResponse);

    // When
    var result = service.handleMessages(
        List.of(givenDigitalSpecimenEvent(), secondEvent, thirdEvent));

    // Then
    then(repository).should().createDigitalSpecimenRecord(anySet());
    then(fdoRecordBuilder).should(times(3)).createNewHandle(any(DigitalSpecimen.class));
    then(repository).should().rollbackSpecimen(secondSpecimen.id());
    then(fdoRecordBuilder).should().rollbackHandleCreation(secondSpecimen);
    then(kafkaService).should().deadLetterEvent(secondEvent);
    assertThat(result).isEqualTo(List.of(givenDigitalSpecimenRecord(), thirdSpecimen));
  }

  @Test
  void testNewSpecimenKafkaFailed()
      throws Exception {
    // Given
    given(repository.getDigitalSpecimens(List.of(PHYSICAL_SPECIMEN_ID))).willReturn(List.of());
    given(fdoRecordBuilder.createNewHandle(givenDigitalSpecimen())).willReturn(HANDLE);

    given(bulkResponse.errors()).willReturn(false);
    given(
        elasticRepository.indexDigitalSpecimen(Set.of(givenDigitalSpecimenRecord()))).willReturn(
        bulkResponse);
    doThrow(JsonProcessingException.class).when(kafkaService)
        .publishCreateEvent(any(DigitalSpecimenRecord.class));
    given(midsService.calculateMids(givenDigitalSpecimen())).willReturn(1);

    // When
    var result = service.handleMessages(List.of(givenDigitalSpecimenEvent()));

    // Then
    then(repository).should().createDigitalSpecimenRecord(anySet());
    then(elasticRepository).should().rollbackSpecimen(givenDigitalSpecimenRecord());
    then(repository).should().rollbackSpecimen(givenDigitalSpecimenRecord().id());
    then(fdoRecordBuilder).should().rollbackHandleCreation(givenDigitalSpecimenRecord());
    then(kafkaService).should().deadLetterEvent(givenDigitalSpecimenEvent());
    assertThat(result).isEmpty();
  }

  private void givenBulkResponse() {
    var positiveResponse = mock(BulkResponseItem.class);
    given(positiveResponse.error()).willReturn(null);
    given(positiveResponse.id()).willReturn(HANDLE).willReturn(THIRD_HANDLE);
    var negativeResponse = mock(BulkResponseItem.class);
    given(negativeResponse.error()).willReturn(new ErrorCause.Builder().reason("Crashed").build());
    given(negativeResponse.id()).willReturn(SECOND_HANDLE);
    given(bulkResponse.errors()).willReturn(true);
    given(bulkResponse.items()).willReturn(
        List.of(positiveResponse, negativeResponse, positiveResponse));
  }

  @Test
  void testUpdateSpecimenPartialElasticFailed() throws IOException, DisscoRepositoryException {
    // Given
    var secondEvent = givenDigitalSpecimenEvent("Another Specimen");
    var thirdEvent = givenDigitalSpecimenEvent("A third Specimen");
    given(repository.getDigitalSpecimens(
        List.of("A third Specimen", "Another Specimen", PHYSICAL_SPECIMEN_ID)))
        .willReturn(List.of(givenDifferentUnequalSpecimen(THIRD_HANDLE, "A third Specimen"),
            givenDifferentUnequalSpecimen(SECOND_HANDLE, "Another Specimen"),
            givenUnequalDigitalSpecimenRecord()
        ));
    givenBulkResponse();
    given(elasticRepository.indexDigitalSpecimen(anyList())).willReturn(bulkResponse);
    given(midsService.calculateMids(thirdEvent.digitalSpecimen())).willReturn(1);

    // When
    var result = service.handleMessages(
        List.of(givenDigitalSpecimenEvent(), secondEvent, thirdEvent));

    // Then
    then(fdoRecordBuilder).should().updateHandles(anyList());
    then(repository).should(times(2)).createDigitalSpecimenRecord(anyList());
    then(fdoRecordBuilder).should()
        .deleteVersion(givenDifferentUnequalSpecimen(SECOND_HANDLE, "Another Specimen"));
    then(kafkaService).should().deadLetterEvent(secondEvent);
    assertThat(result).hasSize(2);
  }

  @Test
  void testUpdateSpecimenKafkaFailed() throws DisscoRepositoryException, IOException {
    given(repository.getDigitalSpecimens(List.of(PHYSICAL_SPECIMEN_ID))).willReturn(
        List.of(givenUnequalDigitalSpecimenRecord()));
    given(midsService.calculateMids(givenDigitalSpecimen())).willReturn(1);
    given(bulkResponse.errors()).willReturn(false);
    given(
        elasticRepository.indexDigitalSpecimen(List.of(givenDigitalSpecimenRecord(2)))).willReturn(
        bulkResponse);
    doThrow(JsonProcessingException.class).when(kafkaService)
        .publishUpdateEvent(givenDigitalSpecimenRecord(2), givenUnequalDigitalSpecimenRecord());

    // When
    var result = service.handleMessages(List.of(givenDigitalSpecimenEvent()));

    // Then
    then(repository).should(times(2)).createDigitalSpecimenRecord(anyList());
    then(elasticRepository).should().rollbackVersion(givenUnequalDigitalSpecimenRecord());
    then(kafkaService).should().deadLetterEvent(givenDigitalSpecimenEvent());
    assertThat(result).isEmpty();
  }

  @Test
  void testUpdateSpecimenIOException() throws IOException, DisscoRepositoryException {
    // Given
    var unequalCurrentDigitalSpecimen = givenUnequalDigitalSpecimenRecord(ANOTHER_ORGANISATION);
    given(repository.getDigitalSpecimens(List.of(PHYSICAL_SPECIMEN_ID))).willReturn(
        List.of(unequalCurrentDigitalSpecimen));
    given(
        elasticRepository.indexDigitalSpecimen(List.of(givenDigitalSpecimenRecord(2)))).willThrow(
        IOException.class);
    given(midsService.calculateMids(givenDigitalSpecimen())).willReturn(1);

    // When
    var result = service.handleMessages(List.of(givenDigitalSpecimenEvent()));

    // Then
    then(fdoRecordBuilder).should().updateHandles(List.of(
        new UpdatedDigitalSpecimenTuple(unequalCurrentDigitalSpecimen,
            givenDigitalSpecimenEvent())));
    then(repository).should(times(2)).createDigitalSpecimenRecord(anyList());
    then(fdoRecordBuilder).should().deleteVersion(unequalCurrentDigitalSpecimen);
    then(kafkaService).should().deadLetterEvent(givenDigitalSpecimenEvent());
    assertThat(result).isEmpty();
  }

  @Test
  void testNewSpecimenError() throws Exception {
    // Given
    given(repository.getDigitalSpecimens(List.of(PHYSICAL_SPECIMEN_ID))).willReturn(List.of());
    given(fdoRecordBuilder.createNewHandle(givenDigitalSpecimen())).willThrow(
        TransformerException.class);

    // When
    var result = service.handleMessages(List.of(givenDigitalSpecimenEvent()));

    // Then
    then(repository).shouldHaveNoMoreInteractions();
    then(elasticRepository).shouldHaveNoInteractions();
    assertThat(result).isEmpty();
  }

  @Test
  void testFailedToRetrieveCurrentSpecimen()
      throws DisscoRepositoryException, JsonProcessingException {
    // Given
    given(repository.getDigitalSpecimens(List.of(PHYSICAL_SPECIMEN_ID))).willThrow(
        DisscoRepositoryException.class);

    // When
    var result = service.handleMessages(List.of(givenDigitalSpecimenEvent()));

    // Then
    assertThat(result).isEmpty();
    then(kafkaService).should().republishEvent(givenDigitalSpecimenEvent());
    then(kafkaService).shouldHaveNoMoreInteractions();
    then(fdoRecordBuilder).shouldHaveNoInteractions();
  }

}
