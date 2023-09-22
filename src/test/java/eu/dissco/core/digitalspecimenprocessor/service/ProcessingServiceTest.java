package eu.dissco.core.digitalspecimenprocessor.service;

import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.AAS;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.ANOTHER_ORGANISATION;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.ANOTHER_SPECIMEN_NAME;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.CREATED;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.HANDLE;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MAPPER;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.PHYSICAL_SPECIMEN_ID;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.SECOND_HANDLE;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.THIRD_HANDLE;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDifferentUnequalSpecimen;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalMediaEventWithRelationship;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimen;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenEvent;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenRecord;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenHandleComponentResponse;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenUnequalDigitalSpecimenRecord;
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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import co.elastic.clients.elasticsearch._types.ErrorCause;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import com.fasterxml.jackson.core.JsonProcessingException;
import eu.dissco.core.digitalspecimenprocessor.domain.DigitalMediaObjectEvent;
import eu.dissco.core.digitalspecimenprocessor.domain.DigitalSpecimen;
import eu.dissco.core.digitalspecimenprocessor.domain.DigitalSpecimenEvent;
import eu.dissco.core.digitalspecimenprocessor.domain.DigitalSpecimenRecord;
import eu.dissco.core.digitalspecimenprocessor.exception.DisscoRepositoryException;
import eu.dissco.core.digitalspecimenprocessor.exception.PidCreationException;
import eu.dissco.core.digitalspecimenprocessor.repository.DigitalSpecimenRepository;
import eu.dissco.core.digitalspecimenprocessor.repository.ElasticSearchRepository;
import eu.dissco.core.digitalspecimenprocessor.web.HandleComponent;
import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Set;
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
  private FdoRecordService fdoRecordService;
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
    service = new ProcessingService(repository, fdoRecordService, elasticRepository, kafkaService,
        midsService, handleComponent);
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
  void testEqualSpecimen() throws DisscoRepositoryException, JsonProcessingException {
    // Given
    given(repository.getDigitalSpecimens(List.of(PHYSICAL_SPECIMEN_ID))).willReturn(
        List.of(givenDigitalSpecimenRecord()));

    // When
    List<DigitalSpecimenRecord> result = service.handleMessages(
        List.of(givenDigitalSpecimenEvent(true)));

    // Then
    verifyNoInteractions(handleComponent);
    verifyNoInteractions(fdoRecordService);
    then(repository).should().updateLastChecked(List.of(HANDLE));
    then(kafkaService).should(times(2))
        .publishDigitalMediaObject(givenDigitalMediaEventWithRelationship());
    assertThat(result).isEmpty();
  }

  @Test
  void testUnequalSpecimen() throws Exception {
    // Given
    var expected = List.of(givenDigitalSpecimenRecord(2));
    given(repository.getDigitalSpecimens(List.of(PHYSICAL_SPECIMEN_ID))).willReturn(
        List.of(givenUnequalDigitalSpecimenRecord()));
    given(bulkResponse.errors()).willReturn(false);
    given(
        elasticRepository.indexDigitalSpecimen(expected)).willReturn(
        bulkResponse);
    given(midsService.calculateMids(givenDigitalSpecimen())).willReturn(1);
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);

    // When
    var result = service.handleMessages(List.of(givenDigitalSpecimenEvent(true)));

    // Then
    then(fdoRecordService).should()
        .buildPostHandleRequest(List.of(expected.get(0).digitalSpecimen()));
    then(handleComponent).should().postHandle(any());
    then(repository).should().createDigitalSpecimenRecord(expected);
    then(kafkaService).should()
        .publishUpdateEvent(givenDigitalSpecimenRecord(2), givenUnequalDigitalSpecimenRecord());
    then(kafkaService).should(times(2))
        .publishDigitalMediaObject(givenDigitalMediaEventWithRelationship());
    assertThat(result).isEqualTo(List.of(givenDigitalSpecimenRecord(2)));
  }

  @Test
  void testHandleRecordDoesNotNeedUpdate() throws Exception {
    // Given
    var expected = List.of(givenDigitalSpecimenRecord(2));
    given(repository.getDigitalSpecimens(List.of(PHYSICAL_SPECIMEN_ID))).willReturn(
        List.of(givenUnequalDigitalSpecimenRecord()));
    given(bulkResponse.errors()).willReturn(false);
    given(
        elasticRepository.indexDigitalSpecimen(expected)).willReturn(
        bulkResponse);
    given(midsService.calculateMids(givenDigitalSpecimen())).willReturn(1);
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(false);

    // When
    var result = service.handleMessages(List.of(givenDigitalSpecimenEvent(true)));

    // Then
    verifyNoMoreInteractions(fdoRecordService);
    verifyNoMoreInteractions(handleComponent);
    then(repository).should().createDigitalSpecimenRecord(expected);
    then(kafkaService).should()
        .publishUpdateEvent(givenDigitalSpecimenRecord(2), givenUnequalDigitalSpecimenRecord());
    then(kafkaService).should(times(2))
        .publishDigitalMediaObject(givenDigitalMediaEventWithRelationship());
    assertThat(result).isEqualTo(List.of(givenDigitalSpecimenRecord(2)));
  }

  @Test
  void testNewSpecimen() throws Exception {
    // Given
    given(repository.getDigitalSpecimens(List.of(PHYSICAL_SPECIMEN_ID))).willReturn(List.of());
    given(bulkResponse.errors()).willReturn(false);
    given(
        elasticRepository.indexDigitalSpecimen(Set.of(givenDigitalSpecimenRecord()))).willReturn(
        bulkResponse);
    given(midsService.calculateMids(givenDigitalSpecimen())).willReturn(1);
    given(fdoRecordService.buildPostHandleRequest(List.of(givenDigitalSpecimen()))).willReturn(
        List.of(MAPPER.createObjectNode()));
    given(handleComponent.postHandle(anyList()))
        .willReturn(givenHandleComponentResponse(List.of(givenDigitalSpecimenRecord())));

    // When
    var result = service.handleMessages(List.of(givenDigitalSpecimenEvent(true)));

    // Then
    then(repository).should().createDigitalSpecimenRecord(Set.of(givenDigitalSpecimenRecord()));
    then(kafkaService).should().publishCreateEvent(givenDigitalSpecimenRecord());
    then(kafkaService).should().publishAnnotationRequestEvent(AAS, givenDigitalSpecimenRecord());
    then(kafkaService).should(times(2))
        .publishDigitalMediaObject(givenDigitalMediaEventWithRelationship());
    assertThat(result).isEqualTo(List.of(givenDigitalSpecimenRecord()));
  }

  @Test
  void testNewSpecimenPidFailed() throws Exception {
    // Given
    given(repository.getDigitalSpecimens(List.of(PHYSICAL_SPECIMEN_ID))).willReturn(List.of());
    given(handleComponent.postHandle(anyList())).willThrow(PidCreationException.class);

    // When
    var result = service.handleMessages(List.of(givenDigitalSpecimenEvent()));

    // Then
    then(kafkaService).should().deadLetterEvent(givenDigitalSpecimenEvent());
    assertThat(result).isEmpty();
  }

  @Test
  void testNewSpecimenPidAndKafkaFailed() throws Exception {
    // Given
    given(repository.getDigitalSpecimens(List.of(PHYSICAL_SPECIMEN_ID))).willReturn(List.of());
    given(handleComponent.postHandle(anyList())).willThrow(PidCreationException.class);
    doThrow(JsonProcessingException.class).when(kafkaService)
        .deadLetterEvent(givenDigitalSpecimenEvent());

    // When
    var result = service.handleMessages(List.of(givenDigitalSpecimenEvent()));

    // Then
    then(handleComponent).should().rollbackFromPhysId(List.of(PHYSICAL_SPECIMEN_ID));
    assertThat(result).isEmpty();
  }

  @Test
  void testDuplicateNewSpecimen()
      throws Exception {
    // Given
    var duplicateSpecimen = new DigitalSpecimenEvent(List.of(AAS),
        givenDigitalSpecimen(PHYSICAL_SPECIMEN_ID, ANOTHER_SPECIMEN_NAME, ANOTHER_ORGANISATION),
        List.of());
    given(repository.getDigitalSpecimens(List.of(PHYSICAL_SPECIMEN_ID))).willReturn(List.of());
    given(bulkResponse.errors()).willReturn(false);
    given(
        elasticRepository.indexDigitalSpecimen(Set.of(givenDigitalSpecimenRecord()))).willReturn(
        bulkResponse);
    given(midsService.calculateMids(givenDigitalSpecimen())).willReturn(1);
    given(fdoRecordService.buildPostHandleRequest(List.of(givenDigitalSpecimen()))).willReturn(
        List.of(MAPPER.createObjectNode()));
    given(handleComponent.postHandle(anyList()))
        .willReturn(givenHandleComponentResponse(List.of(givenDigitalSpecimenRecord())));

    // When
    var result = service.handleMessages(
        List.of(givenDigitalSpecimenEvent(), duplicateSpecimen));

    // Then
    verify(handleComponent, times(1)).postHandle(anyList());
    then(repository).should().createDigitalSpecimenRecord(Set.of(givenDigitalSpecimenRecord()));
    then(kafkaService).should().publishCreateEvent(givenDigitalSpecimenRecord());
    then(kafkaService).should().publishAnnotationRequestEvent(AAS, givenDigitalSpecimenRecord());
    then(kafkaService).should().republishEvent(duplicateSpecimen);
    assertThat(result).isEqualTo(List.of(givenDigitalSpecimenRecord()));
  }

  @Test
  void testNewSpecimenRollbackHandleCreationFailed() throws Exception {
    // Given
    given(repository.getDigitalSpecimens(List.of(PHYSICAL_SPECIMEN_ID))).willReturn(List.of());
    given(midsService.calculateMids(givenDigitalSpecimen())).willReturn(1);
    given(
        elasticRepository.indexDigitalSpecimen(Set.of(givenDigitalSpecimenRecord()))).willThrow(
        IOException.class);
    doThrow(PidCreationException.class).when(handleComponent).rollbackHandleCreation(any());
    given(handleComponent.postHandle(anyList()))
        .willReturn(givenHandleComponentResponse(List.of(givenDigitalSpecimenRecord())));

    // When
    var result = service.handleMessages(List.of(givenDigitalSpecimenEvent()));

    // Then
    then(repository).should().createDigitalSpecimenRecord(Set.of(givenDigitalSpecimenRecord()));
    then(repository).should().rollbackSpecimen(givenDigitalSpecimenRecord().id());
    then(kafkaService).should().deadLetterEvent(givenDigitalSpecimenEvent());
    assertThat(result).isEmpty();
  }

  @Test
  void testNewSpecimenIOException()
      throws Exception {
    // Given
    given(repository.getDigitalSpecimens(List.of(PHYSICAL_SPECIMEN_ID))).willReturn(List.of());
    given(
        elasticRepository.indexDigitalSpecimen(Set.of(givenDigitalSpecimenRecord()))).willThrow(
        IOException.class);
    given(midsService.calculateMids(givenDigitalSpecimen())).willReturn(1);
    given(handleComponent.postHandle(anyList()))
        .willReturn(givenHandleComponentResponse(List.of(givenDigitalSpecimenRecord())));

    // When
    var result = service.handleMessages(List.of(givenDigitalSpecimenEvent()));

    // Then
    then(repository).should().createDigitalSpecimenRecord(Set.of(givenDigitalSpecimenRecord()));
    then(repository).should().rollbackSpecimen(givenDigitalSpecimenRecord().id());
    then(handleComponent).should().rollbackHandleCreation(any());
    then(kafkaService).should().deadLetterEvent(givenDigitalSpecimenEvent());
    assertThat(result).isEmpty();
  }

  @Test
  void testNewSpecimenPartialElasticFailed()
      throws Exception {
    // Given
    String secondPhysicalId = "Another Specimen";
    String thirdPhysicalId = "A third Specimen";
    var secondEvent = givenDigitalSpecimenEvent(secondPhysicalId);
    var secondSpecimen = givenDigitalSpecimenRecord(SECOND_HANDLE, secondPhysicalId);
    var thirdEvent = givenDigitalSpecimenEvent(thirdPhysicalId);
    var thirdSpecimen = givenDigitalSpecimenRecord(THIRD_HANDLE, thirdPhysicalId);
    var specimenRecordList = List.of(
        givenDigitalSpecimenRecord(),
        givenDigitalSpecimenRecord(SECOND_HANDLE, secondPhysicalId),
        givenDigitalSpecimenRecord(THIRD_HANDLE, thirdPhysicalId)
    );
    given(repository.getDigitalSpecimens(anyList())).willReturn(List.of());
    given(midsService.calculateMids(any(DigitalSpecimen.class))).willReturn(1);
    givenBulkResponse();
    given(elasticRepository.indexDigitalSpecimen(anySet())).willReturn(bulkResponse);
    given(handleComponent.postHandle(anyList()))
        .willReturn(givenHandleComponentResponse(specimenRecordList));

    // When
    var result = service.handleMessages(
        List.of(givenDigitalSpecimenEvent(), secondEvent, thirdEvent));

    // Then
    then(repository).should().createDigitalSpecimenRecord(anySet());
    then(repository).should().rollbackSpecimen(secondSpecimen.id());
    then(fdoRecordService).should().buildRollbackCreationRequest(List.of(secondSpecimen));
    then(handleComponent).should().rollbackHandleCreation(any());
    then(handleComponent).should().postHandle(any());
    then(kafkaService).should().deadLetterEvent(secondEvent);
    then(kafkaService).should(times(2))
        .publishDigitalMediaObject(any(DigitalMediaObjectEvent.class));
    assertThat(result).contains(givenDigitalSpecimenRecord(), thirdSpecimen);
  }

  @Test
  void testNewSpecimenKafkaFailed()
      throws Exception {
    // Given
    given(repository.getDigitalSpecimens(List.of(PHYSICAL_SPECIMEN_ID))).willReturn(List.of());
    given(fdoRecordService.buildPostHandleRequest(List.of(givenDigitalSpecimen()))).willReturn(
        List.of(MAPPER.createObjectNode()));
    given(handleComponent.postHandle(anyList()))
        .willReturn(givenHandleComponentResponse(List.of(givenDigitalSpecimenRecord())));
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
    then(handleComponent).should().rollbackHandleCreation(any());
    then(kafkaService).should().deadLetterEvent(givenDigitalSpecimenEvent());
    then(kafkaService).shouldHaveNoMoreInteractions();
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
  void testUpdateSpecimenHandleFailed() throws Exception {
    var secondEvent = givenDigitalSpecimenEvent("Another Specimen");
    var thirdEvent = givenDigitalSpecimenEvent("A third Specimen");
    given(repository.getDigitalSpecimens(anyList()))
        .willReturn(List.of(givenDifferentUnequalSpecimen(THIRD_HANDLE, "A third Specimen"),
            givenDifferentUnequalSpecimen(SECOND_HANDLE, "Another Specimen"),
            givenUnequalDigitalSpecimenRecord()
        ));
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);
    doThrow(PidCreationException.class).when(handleComponent).postHandle(any());

    // When
    var result = service.handleMessages(
        List.of(givenDigitalSpecimenEvent(), secondEvent, thirdEvent));

    // Then
    then(kafkaService).should().deadLetterEvent(givenDigitalSpecimenEvent());
    then(kafkaService).should().deadLetterEvent(secondEvent);
    then(kafkaService).should().deadLetterEvent(thirdEvent);
    then(kafkaService).shouldHaveNoMoreInteractions();
    assertThat(result).isEmpty();
  }

  @Test
  void testUpdateSpecimenHandleAndKafkaFailed() throws Exception {
    var secondEvent = givenDigitalSpecimenEvent("Another Specimen");
    var thirdEvent = givenDigitalSpecimenEvent("A third Specimen");
    given(repository.getDigitalSpecimens(anyList()))
        .willReturn(List.of(givenDifferentUnequalSpecimen(THIRD_HANDLE, "A third Specimen"),
            givenDifferentUnequalSpecimen(SECOND_HANDLE, "Another Specimen"),
            givenUnequalDigitalSpecimenRecord()
        ));
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);
    doThrow(PidCreationException.class).when(handleComponent).postHandle(any());
    doThrow(JsonProcessingException.class).when(kafkaService).deadLetterEvent(any());

    // When
    var result = service.handleMessages(
        List.of(givenDigitalSpecimenEvent(), secondEvent, thirdEvent));

    // Then
    assertThat(result).isEmpty();
  }

  @Test
  void testUpdateSpecimenPartialElasticFailed() throws Exception {
    // Given
    var firstEvent = givenDigitalSpecimenEvent(true);
    var secondEvent = givenDigitalSpecimenEvent("Another Specimen");
    var thirdEvent = givenDigitalSpecimenEvent("A third Specimen");
    given(repository.getDigitalSpecimens(anyList()))
        .willReturn(List.of(givenDifferentUnequalSpecimen(THIRD_HANDLE, "A third Specimen"),
            givenDifferentUnequalSpecimen(SECOND_HANDLE, "Another Specimen"),
            givenUnequalDigitalSpecimenRecord()
        ));
    givenBulkResponse();
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);
    given(elasticRepository.indexDigitalSpecimen(anyList())).willReturn(bulkResponse);
    given(midsService.calculateMids(firstEvent.digitalSpecimen())).willReturn(1);

    // When
    var result = service.handleMessages(List.of(firstEvent, secondEvent, thirdEvent));

    // Then
    then(handleComponent).should().postHandle(anyList());
    then(handleComponent).should().rollbackHandleUpdate(any());
    then(repository).should(times(2)).createDigitalSpecimenRecord(anyList());
    then(kafkaService).should().deadLetterEvent(secondEvent);
    then(kafkaService).should(times(4))
        .publishDigitalMediaObject(any(DigitalMediaObjectEvent.class));
    assertThat(result).hasSize(2);
  }

  @Test
  void testUpdateSpecimenPartialElasticAndRollbackFailed() throws Exception {
    // Given
    var firstEvent = givenDigitalSpecimenEvent(true);
    var secondEvent = givenDigitalSpecimenEvent("Another Specimen");
    var thirdEvent = givenDigitalSpecimenEvent("A third Specimen");
    given(repository.getDigitalSpecimens(
        List.of(PHYSICAL_SPECIMEN_ID, "A third Specimen", "Another Specimen")))
        .willReturn(List.of(
            givenUnequalDigitalSpecimenRecord(),
            givenDifferentUnequalSpecimen(THIRD_HANDLE, "A third Specimen"),
            givenDifferentUnequalSpecimen(SECOND_HANDLE, "Another Specimen")));
    givenBulkResponse();
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);
    given(elasticRepository.indexDigitalSpecimen(anyList())).willReturn(bulkResponse);
    given(midsService.calculateMids(firstEvent.digitalSpecimen())).willReturn(1);
    doThrow(PidCreationException.class).when(handleComponent).rollbackHandleUpdate(any());

    // When
    var result = service.handleMessages(List.of(firstEvent, secondEvent, thirdEvent));

    // Then
    then(fdoRecordService).should().buildRollbackUpdateRequest(
        List.of(givenDifferentUnequalSpecimen(SECOND_HANDLE, "Another Specimen")));
    then(handleComponent).should().postHandle(anyList());
    then(handleComponent).should().rollbackHandleUpdate(any());
    then(repository).should(times(2)).createDigitalSpecimenRecord(anyList());
    then(kafkaService).should().deadLetterEvent(secondEvent);
    then(kafkaService).should(times(4))
        .publishDigitalMediaObject(any(DigitalMediaObjectEvent.class));
    assertThat(result).hasSize(2);
  }

  @Test
  void testUpdateSpecimenKafkaFailed() throws Exception {
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
    then(fdoRecordService).should().buildRollbackUpdateRequest(any());
    assertThat(result).isEmpty();
  }

  @Test
  void testUpdateSpecimenIOException() throws Exception {
    // Given
    var unequalCurrentDigitalSpecimen = givenUnequalDigitalSpecimenRecord(ANOTHER_ORGANISATION);
    given(repository.getDigitalSpecimens(List.of(PHYSICAL_SPECIMEN_ID))).willReturn(
        List.of(unequalCurrentDigitalSpecimen));
    given(
        elasticRepository.indexDigitalSpecimen(List.of(givenDigitalSpecimenRecord(2)))).willThrow(
        IOException.class);
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);
    given(midsService.calculateMids(givenDigitalSpecimen())).willReturn(1);

    // When
    var result = service.handleMessages(List.of(givenDigitalSpecimenEvent()));

    // Then
    then(fdoRecordService).should()
        .buildPostHandleRequest(List.of(givenDigitalSpecimenRecord(2).digitalSpecimen()));
    then(fdoRecordService).should()
        .buildRollbackUpdateRequest(List.of(unequalCurrentDigitalSpecimen));
    then(handleComponent).should().postHandle(any());
    then(handleComponent).should().rollbackHandleUpdate(any());
    then(repository).should(times(2)).createDigitalSpecimenRecord(anyList());
    then(handleComponent).should().rollbackHandleUpdate(any());
    then(kafkaService).should().deadLetterEvent(givenDigitalSpecimenEvent());
    assertThat(result).isEmpty();
  }

  @Test
  void testNewSpecimenPidCreationException() throws Exception {
    // Given
    given(repository.getDigitalSpecimens(List.of(PHYSICAL_SPECIMEN_ID))).willReturn(List.of());
    doThrow(PidCreationException.class).when(handleComponent).postHandle(any());

    // When
    var result = service.handleMessages(List.of(givenDigitalSpecimenEvent()));

    // Then
    then(repository).shouldHaveNoMoreInteractions();
    then(elasticRepository).shouldHaveNoInteractions();
    assertThat(result).isEmpty();
  }

  @Test
  void testNewSpecimenError() throws Exception {
    // Given
    given(repository.getDigitalSpecimens(List.of(PHYSICAL_SPECIMEN_ID))).willReturn(List.of());

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
    then(fdoRecordService).shouldHaveNoInteractions();
  }

}
