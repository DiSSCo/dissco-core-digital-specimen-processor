package eu.dissco.core.digitalspecimenprocessor.service;

import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.CREATED;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.HANDLE;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MAPPER;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.PHYSICAL_SPECIMEN_ID;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.PHYSICAL_SPECIMEN_ID_ALT;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.SECOND_HANDLE;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenEvent;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenRecord;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenRecordWithMediaEr;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenEmptyMediaProcessResult;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenJsonPatch;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenPidProcessResultSpecimen;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenUpdatedDigitalSpecimenRecord;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenUpdatedDigitalSpecimenTuple;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mockStatic;

import co.elastic.clients.elasticsearch.core.BulkResponse;
import com.fasterxml.jackson.core.JsonProcessingException;
import eu.dissco.core.digitalspecimenprocessor.domain.relation.PidProcessResult;
import eu.dissco.core.digitalspecimenprocessor.exception.PidException;
import eu.dissco.core.digitalspecimenprocessor.repository.DigitalSpecimenRepository;
import eu.dissco.core.digitalspecimenprocessor.repository.ElasticSearchRepository;
import eu.dissco.core.digitalspecimenprocessor.web.HandleComponent;
import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.jooq.exception.DataAccessException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DigitalSpecimenServiceTest {

  @Mock
  private DigitalSpecimenRepository repository;
  @Mock
  private RollbackService rollbackService;
  @Mock
  private ElasticSearchRepository elasticRepository;
  @Mock
  private FdoRecordService fdoRecordService;
  @Mock
  private RabbitMqPublisherService publisherService;
  @Mock
  private HandleComponent handleComponent;
  @Mock
  private AnnotationPublisherService annotationPublisherService;
  @Mock
  private MidsService midsService;
  @Mock
  private BulkResponse bulkResponse;

  private DigitalSpecimenService digitalSpecimenService;

  @BeforeEach
  void setUp() {
    digitalSpecimenService = new DigitalSpecimenService(repository, rollbackService,
        elasticRepository, fdoRecordService, publisherService, handleComponent,
        annotationPublisherService, midsService, MAPPER);
  }

  @BeforeAll
  static void init() {
    Clock clock = Clock.fixed(CREATED, ZoneOffset.UTC);
    Instant instant = Instant.now(clock);
    MockedStatic<Instant> mockedInstant = mockStatic(Instant.class);
    mockedInstant.when(Instant::now).thenReturn(instant);
    mockedInstant.when(() -> Instant.from(any())).thenReturn(instant);
    mockedInstant.when(() -> Instant.parse(any())).thenReturn(instant);
    MockedStatic<Clock> mockedClock = mockStatic(Clock.class);
    mockedClock.when(Clock::systemUTC).thenReturn(clock);
  }

  @Test
  void testUpdateEqualSpecimen() {
    // Given

    // When
    digitalSpecimenService.updateEqualSpecimen(List.of(givenDigitalSpecimenRecord()));

    // Then
    then(repository).should().updateLastChecked(List.of(HANDLE));

  }

  @Test
  void testNewSpecimen() throws Exception {
    // Given
    var events = List.of(givenDigitalSpecimenEvent());
    var records = Set.of(givenDigitalSpecimenRecord());
    var pidMap = Map.of(PHYSICAL_SPECIMEN_ID, givenPidProcessResultSpecimen(false));
    given(midsService.calculateMids(any())).willReturn(1);
    given(bulkResponse.errors()).willReturn(false);
    given(elasticRepository.indexDigitalSpecimen(records)).willReturn(bulkResponse);

    // When
    var results = digitalSpecimenService.createNewDigitalSpecimen(events, pidMap);

    // Then
    assertThat(results).isEqualTo(records);
    then(repository).should().createDigitalSpecimenRecord(records);
    then(annotationPublisherService).should().publishAnnotationNewSpecimen(records);
    then(rollbackService).shouldHaveNoInteractions();
  }

  @Test
  void testNewSpecimenHandleMatchingFails() {
    // Given
    var events = List.of(givenDigitalSpecimenEvent());
    var pidMap = Map.of("physical_id_not_in_the_event_list", givenPidProcessResultSpecimen(false));

    // When
    var results = digitalSpecimenService.createNewDigitalSpecimen(events, pidMap);

    // Then
    assertThat(results).isEmpty();
    then(repository).shouldHaveNoInteractions();
    then(elasticRepository).shouldHaveNoInteractions();
    then(annotationPublisherService).shouldHaveNoInteractions();
  }

  @Test
  void testNewSpecimenDatabaseFails() {
    // Given
    var events = List.of(givenDigitalSpecimenEvent());
    var records = Set.of(givenDigitalSpecimenRecord());
    var pidMap = Map.of(PHYSICAL_SPECIMEN_ID, givenPidProcessResultSpecimen(false));
    given(midsService.calculateMids(any())).willReturn(1);
    given(repository.createDigitalSpecimenRecord(records)).willThrow(DataAccessException.class);

    // When
    var results = digitalSpecimenService.createNewDigitalSpecimen(events, pidMap);

    // Then
    assertThat(results).isEmpty();
    then(rollbackService).should().rollbackNewSpecimens(events, pidMap, false, false);
    then(repository).should().createDigitalSpecimenRecord(records);
    then(annotationPublisherService).shouldHaveNoInteractions();
    then(elasticRepository).shouldHaveNoInteractions();
  }

  @Test
  void testNewSpecimenElasticFails() throws Exception {
    // Given
    var events = List.of(givenDigitalSpecimenEvent());
    var records = Set.of(givenDigitalSpecimenRecord());
    var pidMap = Map.of(PHYSICAL_SPECIMEN_ID, givenPidProcessResultSpecimen(false));
    given(midsService.calculateMids(any())).willReturn(1);
    given(elasticRepository.indexDigitalSpecimen(records)).willThrow(IOException.class);

    // When
    var results = digitalSpecimenService.createNewDigitalSpecimen(events, pidMap);

    // Then
    assertThat(results).isEmpty();
    then(rollbackService).should().rollbackNewSpecimens(events, pidMap, false, true);
    then(repository).should().createDigitalSpecimenRecord(records);
    then(annotationPublisherService).shouldHaveNoInteractions();
  }

  @Test
  void testNewSpecimenElasticPartiallyFails() throws Exception {
    // Given
    var events = List.of(givenDigitalSpecimenEvent(),
        givenDigitalSpecimenEvent(PHYSICAL_SPECIMEN_ID_ALT, false));
    var records = Set.of(givenDigitalSpecimenRecord(SECOND_HANDLE, PHYSICAL_SPECIMEN_ID_ALT),
        givenDigitalSpecimenRecord());
    var expected = Set.of(givenDigitalSpecimenRecord());
    var pidMap = Map.of(
        PHYSICAL_SPECIMEN_ID, givenPidProcessResultSpecimen(false),
        PHYSICAL_SPECIMEN_ID_ALT, new PidProcessResult(SECOND_HANDLE, Set.of())
    );
    given(midsService.calculateMids(any())).willReturn(1);
    given(bulkResponse.errors()).willReturn(true);
    given(elasticRepository.indexDigitalSpecimen(records)).willReturn(bulkResponse);
    given(rollbackService.handlePartiallyFailedElasticInsertSpecimen(records, bulkResponse,
        events)).willReturn(expected);

    // When
    var results = digitalSpecimenService.createNewDigitalSpecimen(events, pidMap);

    // Then
    assertThat(results).isEqualTo(expected);
    then(repository).should().createDigitalSpecimenRecord(records);
    then(annotationPublisherService).should().publishAnnotationNewSpecimen(expected);
  }

  @Test
  void testNewSpecimenAnnotationPublicationFails() throws Exception {
    // Given
    var failedRecord = givenDigitalSpecimenRecord();
    var expectedRecord = givenDigitalSpecimenRecord(SECOND_HANDLE, PHYSICAL_SPECIMEN_ID_ALT);
    var events = List.of(givenDigitalSpecimenEvent(),
        givenDigitalSpecimenEvent(PHYSICAL_SPECIMEN_ID_ALT, false));
    var records = Set.of(expectedRecord, failedRecord);
    var pidMap = Map.of(
        PHYSICAL_SPECIMEN_ID, givenPidProcessResultSpecimen(false),
        PHYSICAL_SPECIMEN_ID_ALT, new PidProcessResult(SECOND_HANDLE, Set.of())
    );
    given(midsService.calculateMids(any())).willReturn(1);
    given(bulkResponse.errors()).willReturn(false);
    given(elasticRepository.indexDigitalSpecimen(records)).willReturn(bulkResponse);
    lenient().doThrow(JsonProcessingException.class).when(publisherService)
        .publishCreateEventSpecimen(failedRecord);

    // When
    var results = digitalSpecimenService.createNewDigitalSpecimen(events, pidMap);

    // Then
    assertThat(results).isEqualTo(Set.of(expectedRecord));
    then(rollbackService).should()
        .rollbackNewSpecimensSubset(List.of(failedRecord), events, pidMap, true, true);
  }

  @Test
  void testNewSpecimenWithMedia() throws Exception {
    // Given
    var events = List.of(givenDigitalSpecimenEvent(true));
    var records = Set.of(givenDigitalSpecimenRecordWithMediaEr());
    var pidMap = Map.of(PHYSICAL_SPECIMEN_ID, givenPidProcessResultSpecimen(true));
    given(midsService.calculateMids(any())).willReturn(1);
    given(bulkResponse.errors()).willReturn(false);
    given(elasticRepository.indexDigitalSpecimen(records)).willReturn(bulkResponse);

    // When
    var results = digitalSpecimenService.createNewDigitalSpecimen(events, pidMap);

    // Then
    assertThat(results).isEqualTo(records);
    then(repository).should().createDigitalSpecimenRecord(records);
    then(annotationPublisherService).should().publishAnnotationNewSpecimen(records);
    then(rollbackService).shouldHaveNoInteractions();
  }

  @Test
  void testUpdatedSpecimen() throws Exception {
    // Given
    var tuple = givenUpdatedDigitalSpecimenTuple(false, givenEmptyMediaProcessResult());
    var pidMap = Map.of(PHYSICAL_SPECIMEN_ID, givenPidProcessResultSpecimen(false));
    var expectedRecord = givenDigitalSpecimenRecord(2, false);
    given(fdoRecordService.handleNeedsUpdateSpecimen(any(), any())).willReturn(true);
    given(midsService.calculateMids(any())).willReturn(1);
    given(bulkResponse.errors()).willReturn(false);
    given(elasticRepository.indexDigitalSpecimen(Set.of(expectedRecord))).willReturn(bulkResponse);

    // When
    var result = digitalSpecimenService.updateExistingDigitalSpecimen(List.of(tuple), pidMap);

    // Then
    assertThat(result).isEqualTo(Set.of(expectedRecord));
    then(repository).should().createDigitalSpecimenRecord(Set.of(expectedRecord));
    then(publisherService).should()
        .publishUpdateEventSpecimen(eq(expectedRecord), any());
    then(rollbackService).shouldHaveNoInteractions();
    then(handleComponent).should().updateHandle(any());
  }

  @Test
  void testUpdatedSpecimenDatabaseFails() throws JsonProcessingException {
    // Given
    var tuple = givenUpdatedDigitalSpecimenTuple(false, givenEmptyMediaProcessResult());
    var pidMap = Map.of(PHYSICAL_SPECIMEN_ID, givenPidProcessResultSpecimen(false));
    var expectedRecord = givenDigitalSpecimenRecord(2, false);
    given(midsService.calculateMids(any())).willReturn(1);
    var updatedRecord = givenUpdatedDigitalSpecimenRecord(false);
    doThrow(DataAccessException.class).when(repository)
        .createDigitalSpecimenRecord(Set.of(expectedRecord));

    // When
    var result = digitalSpecimenService.updateExistingDigitalSpecimen(List.of(tuple), pidMap);

    // Then
    assertThat(result).isEmpty();
    then(elasticRepository).shouldHaveNoInteractions();
    then(publisherService).shouldHaveNoInteractions();
    then(rollbackService).should().rollbackUpdatedSpecimens(Set.of(updatedRecord), false, false);
  }

  @Test
  void testUpdatedSpecimenElasticFails() throws Exception {
    // Given
    var tuple = givenUpdatedDigitalSpecimenTuple(false, givenEmptyMediaProcessResult());
    var pidMap = Map.of(PHYSICAL_SPECIMEN_ID, givenPidProcessResultSpecimen(false));
    var updatedRecord = givenUpdatedDigitalSpecimenRecord(false);
    var expectedRecord = givenDigitalSpecimenRecord(2, false);
    given(midsService.calculateMids(any())).willReturn(1);
    doThrow(IOException.class).when(elasticRepository).indexDigitalSpecimen(Set.of(expectedRecord));

    // When
    var result = digitalSpecimenService.updateExistingDigitalSpecimen(List.of(tuple), pidMap);

    // Then
    assertThat(result).isEmpty();
    then(repository).should().createDigitalSpecimenRecord(Set.of(expectedRecord));
    then(publisherService).shouldHaveNoInteractions();
    then(rollbackService).should().rollbackUpdatedSpecimens(Set.of(updatedRecord), false, true);
  }

  @Test
  void testUpdatedSpecimenElasticPartialFails() throws Exception {
    // Given
    var tuple = givenUpdatedDigitalSpecimenTuple(false, givenEmptyMediaProcessResult());
    var pidMap = Map.of(PHYSICAL_SPECIMEN_ID, givenPidProcessResultSpecimen(false));
    var updatedRecord = givenUpdatedDigitalSpecimenRecord(false);
    var expectedRecord = givenDigitalSpecimenRecord(2, false);
    given(midsService.calculateMids(any())).willReturn(1);
    given(bulkResponse.errors()).willReturn(true);
    given(elasticRepository.indexDigitalSpecimen(Set.of(expectedRecord))).willReturn(bulkResponse);

    // When
    var result = digitalSpecimenService.updateExistingDigitalSpecimen(List.of(tuple), pidMap);

    // Then
    assertThat(result).isEmpty();
    then(repository).should().createDigitalSpecimenRecord(Set.of(expectedRecord));
    then(publisherService).shouldHaveNoInteractions();
    then(rollbackService).should()
        .handlePartiallyFailedElasticUpdateSpecimen(Set.of(updatedRecord), bulkResponse);
  }

  @Test
  void testUpdatedSpecimenPublishingFails() throws Exception {
    // Given
    var tuple = givenUpdatedDigitalSpecimenTuple(false, givenEmptyMediaProcessResult());
    var pidMap = Map.of(PHYSICAL_SPECIMEN_ID, givenPidProcessResultSpecimen(false));
    var updatedRecord = givenUpdatedDigitalSpecimenRecord(false);
    var expectedRecord = givenDigitalSpecimenRecord(2, false);
    given(midsService.calculateMids(any())).willReturn(1);
    given(bulkResponse.errors()).willReturn(false);
    given(elasticRepository.indexDigitalSpecimen(Set.of(expectedRecord))).willReturn(bulkResponse);
    doThrow(JsonProcessingException.class).when(publisherService).publishUpdateEventSpecimen(expectedRecord, givenJsonPatch());

    // When
    var result = digitalSpecimenService.updateExistingDigitalSpecimen(List.of(tuple), pidMap);

    // Then
    assertThat(result).isEmpty();
    then(repository).should().createDigitalSpecimenRecord(Set.of(expectedRecord));
    then(rollbackService).should().rollbackUpdatedSpecimens(Set.of(updatedRecord), true, true);
  }

  @Test
  void testUpdatedSpecimenPidFails() throws Exception {
    // Given
    var tuple = givenUpdatedDigitalSpecimenTuple(false, givenEmptyMediaProcessResult());
    var pidMap = Map.of(PHYSICAL_SPECIMEN_ID, givenPidProcessResultSpecimen(false));
    given(fdoRecordService.handleNeedsUpdateSpecimen(any(), any())).willReturn(true);
    doThrow(PidException.class).when(handleComponent).updateHandle(any());

    // When
    var result = digitalSpecimenService.updateExistingDigitalSpecimen(List.of(tuple), pidMap);

    // Then
    assertThat(result).isEmpty();
    then(publisherService).should().deadLetterEventSpecimen(tuple.digitalSpecimenEvent());
  }

  @Test
  void testUpdatedSpecimenPidFailsDlqFails() throws Exception {
    // Given
    var tuple = givenUpdatedDigitalSpecimenTuple(false, givenEmptyMediaProcessResult());
    var pidMap = Map.of(PHYSICAL_SPECIMEN_ID, givenPidProcessResultSpecimen(false));
    given(fdoRecordService.handleNeedsUpdateSpecimen(any(), any())).willReturn(true);
    doThrow(PidException.class).when(handleComponent).updateHandle(any());
    doThrow(JsonProcessingException.class).when(publisherService).deadLetterEventSpecimen(tuple.digitalSpecimenEvent());

    // When
    var result = digitalSpecimenService.updateExistingDigitalSpecimen(List.of(tuple), pidMap);

    // Then
    assertThat(result).isEmpty();
    then(publisherService).should().deadLetterEventSpecimen(tuple.digitalSpecimenEvent());
  }

}
