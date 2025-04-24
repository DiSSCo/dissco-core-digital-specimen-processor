package eu.dissco.core.digitalspecimenprocessor.service;

import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.CREATED;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.HANDLE;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MAPPER;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.PHYSICAL_SPECIMEN_ID;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.PHYSICAL_SPECIMEN_ID_ALT;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.SECOND_HANDLE;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.THIRD_HANDLE;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenEvent;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenRecord;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenPidProcessResultSpecimen;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import co.elastic.clients.elasticsearch._types.ErrorCause;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import eu.dissco.core.digitalspecimenprocessor.domain.relation.PidProcessResult;
import eu.dissco.core.digitalspecimenprocessor.repository.DigitalSpecimenRepository;
import eu.dissco.core.digitalspecimenprocessor.repository.ElasticSearchRepository;
import eu.dissco.core.digitalspecimenprocessor.web.HandleComponent;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.QOM.Pi;
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
  void testNewSpecimenElasticPartiallyFails() throws Exception {
    // Given
    var events = List.of(givenDigitalSpecimenEvent(), givenDigitalSpecimenEvent(PHYSICAL_SPECIMEN_ID_ALT));
    var records = Set.of(givenDigitalSpecimenRecord(SECOND_HANDLE, PHYSICAL_SPECIMEN_ID_ALT), givenDigitalSpecimenRecord());
    var pidMap = Map.of(
        PHYSICAL_SPECIMEN_ID, givenPidProcessResultSpecimen(false),
        PHYSICAL_SPECIMEN_ID_ALT, new PidProcessResult(SECOND_HANDLE, Set.of())
    );
    given(midsService.calculateMids(any())).willReturn(1);
    given(elasticRepository.indexDigitalSpecimen(records)).willReturn(bulkResponse);
    givenBulkResponse();

    // When
    var results = digitalSpecimenService.createNewDigitalSpecimen(events, pidMap);

    // Then
    assertThat(results).isEmpty();
    then(repository).should().createDigitalSpecimenRecord(records);
    then(rollbackService).should().rollbackNewSpecimens(List.of(givenDigitalSpecimenEvent(PHYSICAL_SPECIMEN_ID_ALT)), pidMap, false, true);
    then(repository).should().createDigitalSpecimenRecord(records);
    then(elasticRepository).shouldHaveNoInteractions();
  }

  private void givenBulkResponse() {
    given(bulkResponse.errors()).willReturn(true);
    BulkResponseItem positiveResponse;
    positiveResponse = mock(BulkResponseItem.class);
    given(positiveResponse.error()).willReturn(null);
    given(positiveResponse.id()).willReturn(HANDLE);
    var negativeResponse = mock(BulkResponseItem.class);
    given(negativeResponse.error()).willReturn(new ErrorCause.Builder().reason("Crashed").build());
    given(negativeResponse.id()).willReturn(SECOND_HANDLE);
    given(bulkResponse.items()).willReturn(
        List.of(positiveResponse, negativeResponse, positiveResponse));
  }

}
