package eu.dissco.core.digitalspecimenprocessor.service;

import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.HANDLE;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MEDIA_PID;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MEDIA_URL;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.PHYSICAL_SPECIMEN_ID;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.SECOND_HANDLE;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.THIRD_HANDLE;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDifferentUnequalSpecimen;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalMediaEvent;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenEvent;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenRecord;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenMediaPidResponse;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenUnequalDigitalSpecimenRecord;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenUpdatedDigitalSpecimenRecord;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;

import co.elastic.clients.elasticsearch._types.ErrorCause;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.nimbusds.jose.util.Pair;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaEventWithoutDOI;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaKey;
import eu.dissco.core.digitalspecimenprocessor.exception.PidException;
import eu.dissco.core.digitalspecimenprocessor.repository.DigitalSpecimenRepository;
import eu.dissco.core.digitalspecimenprocessor.repository.ElasticSearchRepository;
import eu.dissco.core.digitalspecimenprocessor.web.HandleComponent;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.jooq.exception.DataAccessException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class RollbackServiceTest {

  @Mock
  ElasticSearchRepository elasticSearchRepository;
  @Mock
  RabbitMqPublisherService rabbitMQService;
  @Mock
  DigitalSpecimenRepository repository;
  @Mock
  FdoRecordService fdoRecordService;
  @Mock
  HandleComponent handleComponent;
  @Mock
  private BulkResponse bulkResponse;
  private RollbackService rollbackService;

  private static final Pair<List<String>, List<DigitalMediaEventWithoutDOI>> MEDIA_EVENT_PAIR = Pair.of(
      List.of("image-metadata"),
      List.of(givenDigitalMediaEvent())
  );

  private static final Pair<List<String>, List<DigitalMediaEventWithoutDOI>> MEDIA_EVENT_PAIR_EMPTY = Pair.of(
      new ArrayList<String>().stream().toList(),
      new ArrayList<DigitalMediaEventWithoutDOI>().stream().toList());

  @BeforeEach
  void init() {
    rollbackService = new RollbackService(elasticSearchRepository, rabbitMQService, repository,
        fdoRecordService, handleComponent);
  }

  /* Rollback New Specimen */

  @Test
  void testRollbackNewSpecimensNoMediaCase1() throws Exception {
    // Given
    var specimenRecords = Map.of(givenDigitalSpecimenRecord(), MEDIA_EVENT_PAIR_EMPTY);

    // When
    rollbackService.rollbackNewSpecimens(specimenRecords, false, false);

    // Then
    then(elasticSearchRepository).shouldHaveNoInteractions();
    then(repository).shouldHaveNoInteractions();
    then(rabbitMQService).should(times(1)).deadLetterEvent(any());
    then(handleComponent).should(times(1)).rollbackHandleCreation(List.of(HANDLE));
  }

  @Test
  void testRollbackNewSpecimensNoMediaCase2() throws Exception {
    // Given
    var specimenRecords = Map.of(givenDigitalSpecimenRecord(), MEDIA_EVENT_PAIR_EMPTY);

    // When
    rollbackService.rollbackNewSpecimens(specimenRecords, false, true);

    // Then
    then(elasticSearchRepository).shouldHaveNoInteractions();
    then(repository).should().rollbackSpecimen(HANDLE);
    then(rabbitMQService).should(times(1)).deadLetterEvent(any());
    then(handleComponent).should(times(1)).rollbackHandleCreation(List.of(HANDLE));
  }

  @Test
  void testRollbackNewSpecimensNoMediaCase3() throws Exception {
    // Given
    var specimenRecords = Map.of(givenDigitalSpecimenRecord(), MEDIA_EVENT_PAIR_EMPTY);

    // When
    rollbackService.rollbackNewSpecimens(specimenRecords, true, true);

    // Then
    then(elasticSearchRepository).should().rollbackSpecimen(givenDigitalSpecimenRecord());
    then(repository).should().rollbackSpecimen(HANDLE);
    then(rabbitMQService).should(times(1)).deadLetterEvent(any());
    then(handleComponent).should(times(1)).rollbackHandleCreation(List.of(HANDLE));
  }

  @Test
  void testRollbackNewSpecimensHasMediaCase1() throws Exception {
    // Given
    var specimenRecords = Map.of(givenDigitalSpecimenRecord(), MEDIA_EVENT_PAIR);

    // When
    rollbackService.rollbackNewSpecimens(specimenRecords, false, false);

    // Then
    then(elasticSearchRepository).shouldHaveNoInteractions();
    then(repository).shouldHaveNoInteractions();
    then(rabbitMQService).should(times(1)).deadLetterEvent(any());
    then(handleComponent).should(times(1)).rollbackHandleCreation(List.of(HANDLE));
    then(handleComponent).should(times(1)).rollbackHandleCreation(List.of(MEDIA_PID));
  }

  @Test
  void testRollbackNewSpecimensHasMediaCase() throws Exception {
    // Given
    var specimenRecords = Map.of(givenDigitalSpecimenRecord(), MEDIA_EVENT_PAIR);

    // When
    rollbackService.rollbackNewSpecimens(specimenRecords, false, true);

    // Then
    then(elasticSearchRepository).shouldHaveNoInteractions();
    then(repository).should().rollbackSpecimen(HANDLE);
    then(rabbitMQService).should(times(1)).deadLetterEvent(any());
    then(handleComponent).should(times(1)).rollbackHandleCreation(List.of(HANDLE));
    then(handleComponent).should(times(1)).rollbackHandleCreation(List.of(MEDIA_PID));
  }

  @Test
  void testRollbackNewSpecimensHasMediaCase3() throws Exception {
    // Given
    var specimenRecords = Map.of(givenDigitalSpecimenRecord(), MEDIA_EVENT_PAIR);

    // When
    rollbackService.rollbackNewSpecimens(specimenRecords, true, true);

    // Then
    then(elasticSearchRepository).should().rollbackSpecimen(givenDigitalSpecimenRecord());
    then(repository).should().rollbackSpecimen(HANDLE);
    then(rabbitMQService).should(times(1)).deadLetterEvent(any());
    then(handleComponent).should(times(1)).rollbackHandleCreation(List.of(HANDLE));
    then(handleComponent).should(times(1)).rollbackHandleCreation(List.of(MEDIA_PID));
  }

  @Test
  void testRollbackNewSpecimensHasMediaCase3ElasticFailed() throws Exception {
    // Given
    var specimenRecords = Map.of(givenDigitalSpecimenRecord(), MEDIA_EVENT_PAIR);
    doThrow(IOException.class).when(elasticSearchRepository)
        .rollbackSpecimen(givenDigitalSpecimenRecord());

    // When
    rollbackService.rollbackNewSpecimens(specimenRecords, true, true);

    // Then
    then(elasticSearchRepository).should().rollbackSpecimen(givenDigitalSpecimenRecord());
    then(repository).should().rollbackSpecimen(HANDLE);
    then(rabbitMQService).should(times(1)).deadLetterEvent(any());
    then(handleComponent).should(times(1)).rollbackHandleCreation(List.of(HANDLE));
    then(handleComponent).should(times(1)).rollbackHandleCreation(List.of(MEDIA_PID));
  }

  @Test
  void testRollbackNewSpecimensHasMediaCase3KafkaFailed() throws Exception {
    // Given
    var specimenRecords = Map.of(givenDigitalSpecimenRecord(), MEDIA_EVENT_PAIR);
    doThrow(JsonProcessingException.class).when(rabbitMQService).deadLetterEvent(any());

    // When
    rollbackService.rollbackNewSpecimens(specimenRecords, true, true);

    // Then
    then(elasticSearchRepository).should().rollbackSpecimen(givenDigitalSpecimenRecord());
    then(repository).should().rollbackSpecimen(HANDLE);
    then(rabbitMQService).should(times(1)).deadLetterEvent(any());
    then(handleComponent).should(times(1)).rollbackHandleCreation(List.of(HANDLE));
    then(handleComponent).should(times(1)).rollbackHandleCreation(List.of(MEDIA_PID));
  }

  @Test
  void testRollbackNewSpecimensHasMediaCase3PidFailed() throws Exception {
    // Given
    var specimenRecords = Map.of(givenDigitalSpecimenRecord(), MEDIA_EVENT_PAIR);
    doThrow(PidException.class).when(handleComponent).rollbackHandleCreation(anyList());

    // When
    rollbackService.rollbackNewSpecimens(specimenRecords, true, true);

    // Then
    then(elasticSearchRepository).should().rollbackSpecimen(givenDigitalSpecimenRecord());
    then(repository).should().rollbackSpecimen(HANDLE);
    then(rabbitMQService).should(times(1)).deadLetterEvent(any());
    then(handleComponent).should(times(1)).rollbackHandleCreation(List.of(HANDLE));
    then(handleComponent).should(times(1)).rollbackHandleCreation(List.of(MEDIA_PID));
  }

  /*  Rollback Updated Specimen */
  @Test
  void rollbackUpdatedSpecimenNoMediaNoUpdateHandleCase1() throws Exception {
    // Given
    var specimenRecords = List.of(givenUpdatedDigitalSpecimenRecord(false));
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(false);

    // When
    rollbackService.rollbackUpdatedSpecimens(specimenRecords, false, false);

    // Then
    then(elasticSearchRepository).shouldHaveNoInteractions();
    then(repository).shouldHaveNoInteractions();
    then(handleComponent).shouldHaveNoInteractions();
    then(rabbitMQService).should(times(1)).deadLetterEvent(any());
  }

  @Test
  void rollbackUpdatedSpecimenNoMediaCase1() throws Exception {
    // Given
    var specimenRecords = List.of(givenUpdatedDigitalSpecimenRecord(false));
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);

    // When
    rollbackService.rollbackUpdatedSpecimens(specimenRecords, false, false);

    // Then
    then(elasticSearchRepository).shouldHaveNoInteractions();
    then(repository).shouldHaveNoInteractions();
    then(handleComponent).should().rollbackHandleUpdate(anyList());
    then(rabbitMQService).should(times(1)).deadLetterEvent(any());
  }

  @Test
  void rollbackUpdatedSpecimenNoMediaCase2() throws Exception {
    // Given
    var specimenRecords = List.of(givenUpdatedDigitalSpecimenRecord(false));
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);

    // When
    rollbackService.rollbackUpdatedSpecimens(specimenRecords, false, true);

    // Then
    then(elasticSearchRepository).shouldHaveNoInteractions();
    then(repository).should().createDigitalSpecimenRecord(List.of(givenDigitalSpecimenRecord()));
    then(handleComponent).should().rollbackHandleUpdate(anyList());
    then(rabbitMQService).should(times(1)).deadLetterEvent(any());
  }

  @Test
  void rollbackUpdatedSpecimenNoMediaCase3() throws Exception {
    // Given
    var specimenRecords = List.of(givenUpdatedDigitalSpecimenRecord(false));
    var originalSpecimen = givenDigitalSpecimenRecord();
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);

    // When
    rollbackService.rollbackUpdatedSpecimens(specimenRecords, true, true);

    // Then
    then(elasticSearchRepository).should().rollbackVersion(originalSpecimen);
    then(repository).should().createDigitalSpecimenRecord(List.of(originalSpecimen));
    then(handleComponent).should().rollbackHandleUpdate(anyList());
    then(rabbitMQService).should(times(1)).deadLetterEvent(any());
  }


  @Test
  void rollbackUpdatedSpecimenHasMediaCase1() throws Exception {
    // Given
    var specimenRecords = List.of(givenUpdatedDigitalSpecimenRecord(false));
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);

    // When
    rollbackService.rollbackUpdatedSpecimens(specimenRecords, false,
        false);

    // Then
    then(elasticSearchRepository).shouldHaveNoInteractions();
    then(repository).shouldHaveNoInteractions();
    then(handleComponent).should().rollbackHandleUpdate(anyList());
    then(handleComponent).should(times(1)).rollbackHandleCreation(List.of(MEDIA_PID));
    then(rabbitMQService).should(times(1)).deadLetterEvent(any());
  }

  @Test
  void rollbackUpdatedSpecimenHasMediaCase2() throws Exception {
    // Given
    var specimenRecords = List.of(givenUpdatedDigitalSpecimenRecord(false));
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);

    // When
    rollbackService.rollbackUpdatedSpecimens(specimenRecords, false, true);

    // Then
    then(elasticSearchRepository).shouldHaveNoInteractions();
    then(repository).should().createDigitalSpecimenRecord(List.of(givenDigitalSpecimenRecord()));
    then(handleComponent).should().rollbackHandleUpdate(anyList());
    then(handleComponent).should(times(1)).rollbackHandleCreation(List.of(MEDIA_PID));
    then(rabbitMQService).should(times(1)).deadLetterEvent(any());
  }

  @Test
  void rollbackUpdatedSpecimenHasMediaCase3() throws Exception {
    // Given
    var specimenRecords = List.of(givenUpdatedDigitalSpecimenRecord(false));
    var originalSpecimen = givenDigitalSpecimenRecord();
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);

    // When
    rollbackService.rollbackUpdatedSpecimens(specimenRecords, true, true);

    // Then
    then(elasticSearchRepository).should().rollbackVersion(originalSpecimen);
    then(repository).should().createDigitalSpecimenRecord(List.of(originalSpecimen));
    then(handleComponent).should().rollbackHandleUpdate(anyList());
    then(handleComponent).should(times(1)).rollbackHandleCreation(List.of(MEDIA_PID));
    then(rabbitMQService).should(times(1)).deadLetterEvent(any());
  }

  @Test
  void rollbackUpdatedSpecimenHasMediaCase3ElasticFailed() throws Exception {
    // Given
    var specimenRecords = List.of(givenUpdatedDigitalSpecimenRecord(false));
    var originalSpecimen = givenDigitalSpecimenRecord();
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);
    doThrow(IOException.class).when(elasticSearchRepository)
        .rollbackVersion(givenDigitalSpecimenRecord());

    // When
    rollbackService.rollbackUpdatedSpecimens(specimenRecords, true, true);

    // Then
    then(elasticSearchRepository).should().rollbackVersion(originalSpecimen);
    then(repository).should().createDigitalSpecimenRecord(List.of(originalSpecimen));
    then(handleComponent).should().rollbackHandleUpdate(anyList());
    then(handleComponent).should(times(1)).rollbackHandleCreation(List.of(MEDIA_PID));
    then(rabbitMQService).should(times(1)).deadLetterEvent(any());
  }

  @Test
  void rollbackUpdatedSpecimenHasMediaCase3KafkaFailed() throws Exception {
    // Given
    var specimenRecords = List.of(givenUpdatedDigitalSpecimenRecord(false));
    var originalSpecimen = givenDigitalSpecimenRecord();
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);
    doThrow(JsonProcessingException.class).when(rabbitMQService).deadLetterEvent(any());

    // When
    rollbackService.rollbackUpdatedSpecimens(specimenRecords, true, true);

    // Then
    then(elasticSearchRepository).should().rollbackVersion(originalSpecimen);
    then(repository).should().createDigitalSpecimenRecord(List.of(originalSpecimen));
    then(handleComponent).should().rollbackHandleUpdate(anyList());
    then(handleComponent).should(times(1)).rollbackHandleCreation(List.of(MEDIA_PID));
    then(rabbitMQService).should(times(1)).deadLetterEvent(any());
  }

  @Test
  void rollbackUpdatedSpecimenHasMediaCase3PidFailed() throws Exception {
    // Given
    var specimenRecords = List.of(givenUpdatedDigitalSpecimenRecord(false));
    var originalSpecimen = givenDigitalSpecimenRecord();
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);
    doThrow(PidException.class).when(handleComponent).rollbackHandleUpdate(any());

    // When
    rollbackService.rollbackUpdatedSpecimens(specimenRecords, true, true);

    // Then
    then(elasticSearchRepository).should().rollbackVersion(originalSpecimen);
    then(repository).should().createDigitalSpecimenRecord(List.of(originalSpecimen));
    then(handleComponent).should().rollbackHandleUpdate(anyList());
    then(handleComponent).should(times(1)).rollbackHandleCreation(List.of(MEDIA_PID));
    then(rabbitMQService).should(times(1)).deadLetterEvent(any());
  }

  @Test
  void rollbackUpdatedSpecimenHasMediaCase3DatabaseFailed() throws Exception {
    // Given
    var specimenRecords = List.of(givenUpdatedDigitalSpecimenRecord(false));
    var originalSpecimen = givenDigitalSpecimenRecord();
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);
    doThrow(DataAccessException.class).when(repository).createDigitalSpecimenRecord(any());

    // When
    rollbackService.rollbackUpdatedSpecimens(specimenRecords, true, true);

    // Then
    then(elasticSearchRepository).should().rollbackVersion(originalSpecimen);
    then(repository).should().createDigitalSpecimenRecord(List.of(originalSpecimen));
    then(handleComponent).should().rollbackHandleUpdate(anyList());
    then(handleComponent).should(times(1)).rollbackHandleCreation(List.of(MEDIA_PID));
    then(rabbitMQService).should(times(1)).deadLetterEvent(any());
  }

  /* Elastic Insert Failures */

  @Test
  void testHandlePartiallyFailedInsertNoMedia() throws Exception {
    // Given
    var specimenRecords = Map.of(
        givenDigitalSpecimenRecord(), MEDIA_EVENT_PAIR_EMPTY,
        givenDigitalSpecimenRecord(SECOND_HANDLE), MEDIA_EVENT_PAIR_EMPTY,
        givenDigitalSpecimenRecord(THIRD_HANDLE), MEDIA_EVENT_PAIR_EMPTY
    );
    givenBulkResponse();

    // When
    var result = rollbackService.handlePartiallyFailedElasticInsert(specimenRecords, Map.of(),
        bulkResponse);

    // Then
    then(handleComponent).should().rollbackHandleCreation(List.of(SECOND_HANDLE));
    then(rabbitMQService).should(times(2)).publishCreateEvent(any());
    then(rabbitMQService).should(times(1)).deadLetterEvent(any());
    then(repository).should().rollbackSpecimen(SECOND_HANDLE);
    then(elasticSearchRepository).shouldHaveNoMoreInteractions();
    assertThat(result).hasSize(2);
  }

  @Test
  void testHandlePartiallyFailedInsertNoMediaPublishingFailed() throws Exception {
    // Given
    var firstRecord = givenDigitalSpecimenRecord();
    var specimenRecords = Map.of(
        firstRecord, MEDIA_EVENT_PAIR_EMPTY,
        givenDigitalSpecimenRecord(SECOND_HANDLE), MEDIA_EVENT_PAIR_EMPTY,
        givenDigitalSpecimenRecord(THIRD_HANDLE), MEDIA_EVENT_PAIR_EMPTY
    );

    givenBulkResponse();
    doThrow(JsonProcessingException.class).when(rabbitMQService).publishCreateEvent(firstRecord);

    // When
    var result = rollbackService.handlePartiallyFailedElasticInsert(specimenRecords, Map.of(),
        bulkResponse);

    // Then
    then(handleComponent).should().rollbackHandleCreation(List.of(HANDLE, SECOND_HANDLE));
    then(rabbitMQService).should(times(2)).publishCreateEvent(any());
    then(rabbitMQService).should(times(2)).deadLetterEvent(any());
    then(repository).should().rollbackSpecimen(SECOND_HANDLE);
    then(repository).should().rollbackSpecimen(HANDLE);
    then(repository).shouldHaveNoMoreInteractions();
    then(elasticSearchRepository).should().rollbackSpecimen(firstRecord);
    then(elasticSearchRepository).shouldHaveNoMoreInteractions();
    assertThat(result).hasSize(1);
  }

  @Test
  void testHandlePartiallyFailedInsertFailureHasMedia() throws Exception {
    // Given
    var specimenRecords = Map.of(
        givenDigitalSpecimenRecord(), MEDIA_EVENT_PAIR_EMPTY,
        givenDigitalSpecimenRecord(SECOND_HANDLE), MEDIA_EVENT_PAIR,
        givenDigitalSpecimenRecord(THIRD_HANDLE), MEDIA_EVENT_PAIR_EMPTY
    );
    givenBulkResponse();

    // When
    var result = rollbackService.handlePartiallyFailedElasticInsert(specimenRecords,
        Map.of(new DigitalMediaKey(SECOND_HANDLE, MEDIA_URL), MEDIA_PID), bulkResponse);

    // Then
    then(handleComponent).should().rollbackHandleCreation(List.of(SECOND_HANDLE, MEDIA_PID));
    then(rabbitMQService).should(times(2)).publishCreateEvent(any());
    then(rabbitMQService).should(times(1)).deadLetterEvent(any());
    then(repository).should().rollbackSpecimen(SECOND_HANDLE);
    then(elasticSearchRepository).shouldHaveNoMoreInteractions();
    assertThat(result).hasSize(2);
  }

  @Test
  void testHandlePartiallyFailedInsertSuccessHasMedia() throws Exception {
    // Given
    var specimenRecords = Map.of(
        givenDigitalSpecimenRecord(), MEDIA_EVENT_PAIR,
        givenDigitalSpecimenRecord(SECOND_HANDLE), MEDIA_EVENT_PAIR_EMPTY,
        givenDigitalSpecimenRecord(THIRD_HANDLE), MEDIA_EVENT_PAIR_EMPTY
    );
    givenBulkResponse();

    // When
    var result = rollbackService.handlePartiallyFailedElasticInsert(specimenRecords,
        givenMediaPidResponse(), bulkResponse);

    // Then
    then(handleComponent).should().rollbackHandleCreation(List.of(SECOND_HANDLE));
    then(rabbitMQService).should(times(2)).publishCreateEvent(any());
    then(rabbitMQService).should(times(1)).deadLetterEvent(any());
    then(rabbitMQService).should(times(1)).publishAnnotationRequestEvent(any(), any());
    then(repository).should().rollbackSpecimen(SECOND_HANDLE);
    then(elasticSearchRepository).shouldHaveNoMoreInteractions();
    assertThat(result).hasSize(2);
  }

  @Test
  void testHandlePartiallyFailedInsertSuccessHasMediaAnnotationKafkaFails() throws Exception {
    // Given
    var specimenRecords = Map.of(
        givenDigitalSpecimenRecord(), MEDIA_EVENT_PAIR,
        givenDigitalSpecimenRecord(SECOND_HANDLE), MEDIA_EVENT_PAIR_EMPTY,
        givenDigitalSpecimenRecord(THIRD_HANDLE), MEDIA_EVENT_PAIR_EMPTY
    );
    givenBulkResponse();
    doThrow(JsonProcessingException.class).when(rabbitMQService)
        .publishAnnotationRequestEvent(any(), eq(givenDigitalSpecimenRecord()));

    // When
    var result = rollbackService.handlePartiallyFailedElasticInsert(specimenRecords,
        givenMediaPidResponse(), bulkResponse);

    // Then
    then(handleComponent).should().rollbackHandleCreation(List.of(SECOND_HANDLE));
    then(rabbitMQService).should(times(2)).publishCreateEvent(any());
    then(rabbitMQService).should(times(1)).deadLetterEvent(any());
    then(rabbitMQService).should(times(1)).publishAnnotationRequestEvent(any(), any());
    then(repository).should().rollbackSpecimen(SECOND_HANDLE);
    then(elasticSearchRepository).shouldHaveNoMoreInteractions();
    assertThat(result).hasSize(2);
  }


  private void givenBulkResponse() {
    var positiveResponse = mock(BulkResponseItem.class);
    given(positiveResponse.error()).willReturn(null);
    given(positiveResponse.id()).willReturn(HANDLE).willReturn(THIRD_HANDLE);
    var negativeResponse = mock(BulkResponseItem.class);
    given(negativeResponse.error()).willReturn(new ErrorCause.Builder().reason("Crashed").build());
    given(negativeResponse.id()).willReturn(SECOND_HANDLE);
    given(bulkResponse.items()).willReturn(
        List.of(positiveResponse, negativeResponse, positiveResponse));
  }

  /* Elastic Update Failure */
  @Test
  void testHandlePartiallyFailedElasticUpdateNoMedia() throws Exception {
    // Given
    var secondSpecimen = givenDifferentUnequalSpecimen(SECOND_HANDLE, "second");
    var secondSpecimenUpdated = givenUpdatedDigitalSpecimenRecord(secondSpecimen, false);
    var thirdSpecimen = givenDifferentUnequalSpecimen(THIRD_HANDLE, "third");
    var specimenRecords = Set.of(
        givenUpdatedDigitalSpecimenRecord(false),
        secondSpecimenUpdated,
        givenUpdatedDigitalSpecimenRecord(thirdSpecimen, false)
    );
    givenBulkResponse();
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);

    // When
    var result = rollbackService.handlePartiallyFailedElasticUpdate(specimenRecords,
        bulkResponse);

    // Then
    assertThat(result).hasSize(2);
    then(rabbitMQService).should(times(2)).publishUpdateEvent(any(), any());
    then(rabbitMQService).should(times(1)).deadLetterEvent(any());
    then(rabbitMQService).shouldHaveNoMoreInteractions();
    then(repository).should()
        .createDigitalSpecimenRecord(List.of(secondSpecimenUpdated.currentDigitalSpecimen()));
    then(elasticSearchRepository).shouldHaveNoMoreInteractions();
    then(handleComponent).should().rollbackHandleUpdate(anyList());
    then(handleComponent).shouldHaveNoMoreInteractions();
  }

  @Test
  void testHandlePartiallyFailedElasticUpdateFailureHasMedia() throws Exception {
    // Given
    var secondSpecimen = givenDifferentUnequalSpecimen(SECOND_HANDLE, "second");
    var secondSpecimenUpdated = givenUpdatedDigitalSpecimenRecord(secondSpecimen, true);
    var thirdSpecimen = givenDifferentUnequalSpecimen(THIRD_HANDLE, "third");
    var specimenRecords = Set.of(
        givenUpdatedDigitalSpecimenRecord(false),
        secondSpecimenUpdated,
        givenUpdatedDigitalSpecimenRecord(thirdSpecimen, false)
    );
    givenBulkResponse();
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);

    // When
    var result = rollbackService.handlePartiallyFailedElasticUpdate(specimenRecords,
        bulkResponse);

    // Then
    assertThat(result).hasSize(2);
    then(rabbitMQService).should(times(2)).publishUpdateEvent(any(), any());
    then(rabbitMQService).should(times(1)).deadLetterEvent(any());
    then(rabbitMQService).shouldHaveNoMoreInteractions();
    then(repository).should()
        .createDigitalSpecimenRecord(List.of(secondSpecimenUpdated.currentDigitalSpecimen()));
    then(elasticSearchRepository).shouldHaveNoMoreInteractions();
    then(handleComponent).should().rollbackHandleUpdate(anyList());
    then(handleComponent).should().rollbackHandleCreation(List.of(MEDIA_PID));
  }

  @Test
  void testHandlePartiallyFailedElasticUpdateSuccessHasMedia() throws Exception {
    // Given
    var secondSpecimen = givenDifferentUnequalSpecimen(SECOND_HANDLE, "second");
    var secondSpecimenUpdated = givenUpdatedDigitalSpecimenRecord(secondSpecimen, false);
    var thirdSpecimen = givenDifferentUnequalSpecimen(THIRD_HANDLE, "third");
    var specimenRecords = Set.of(
        givenUpdatedDigitalSpecimenRecord(true),
        secondSpecimenUpdated,
        givenUpdatedDigitalSpecimenRecord(thirdSpecimen, false)
    );
    givenBulkResponse();
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);

    // When
    var result = rollbackService.handlePartiallyFailedElasticUpdate(specimenRecords,
        bulkResponse);

    // Then
    assertThat(result).hasSize(2);
    then(rabbitMQService).should(times(2)).publishUpdateEvent(any(), any());
    then(rabbitMQService).should(times(1)).deadLetterEvent(any());
    then(rabbitMQService).shouldHaveNoMoreInteractions();
    then(repository).should()
        .createDigitalSpecimenRecord(List.of(secondSpecimenUpdated.currentDigitalSpecimen()));
    then(elasticSearchRepository).shouldHaveNoMoreInteractions();
    then(handleComponent).should().rollbackHandleUpdate(anyList());
    then(handleComponent).shouldHaveNoMoreInteractions();
  }

  @Test
  void testHandlePartiallyFailedElasticUpdateSuccessHasMediaKafkaPublishingFailed()
      throws Exception {
    // Given
    var firstSpecimenOriginal = givenDigitalSpecimenRecord();
    var secondSpecimen = givenDifferentUnequalSpecimen(SECOND_HANDLE, "second");
    var secondSpecimenUpdated = givenUpdatedDigitalSpecimenRecord(secondSpecimen, false);
    var thirdSpecimen = givenDifferentUnequalSpecimen(THIRD_HANDLE, "third");
    var specimenRecords = Set.of(
        givenUpdatedDigitalSpecimenRecord(true),
        secondSpecimenUpdated,
        givenUpdatedDigitalSpecimenRecord(thirdSpecimen, false)
    );
    givenBulkResponse();
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);
    doThrow(JsonProcessingException.class).when(rabbitMQService)
        .publishUpdateEvent(eq(givenUnequalDigitalSpecimenRecord()), any());

    // When
    var result = rollbackService.handlePartiallyFailedElasticUpdate(specimenRecords,
        bulkResponse);

    // Then
    assertThat(result).hasSize(1);
    then(rabbitMQService).should(times(2)).publishUpdateEvent(any(), any());
    then(rabbitMQService).should(times(2)).deadLetterEvent(any());
    then(rabbitMQService).shouldHaveNoMoreInteractions();
    then(repository).should(times(2)).createDigitalSpecimenRecord(anyList());
    then(elasticSearchRepository).should().rollbackVersion(firstSpecimenOriginal);
    then(handleComponent).should().rollbackHandleCreation(List.of(MEDIA_PID));
    then(handleComponent).should().rollbackHandleUpdate(anyList());
  }

  @Test
  void testPidCreationFailed() throws Exception {
    // When
    rollbackService.pidCreationFailed(List.of(givenDigitalSpecimenEvent()));

    // Then
    then(handleComponent).should().rollbackFromPhysId(List.of(PHYSICAL_SPECIMEN_ID));
  }

  @Test
  void testPidCreationFailedRollbackFailed() throws Exception {
    // Given
    doThrow(PidException.class).when(handleComponent)
        .rollbackFromPhysId(List.of(PHYSICAL_SPECIMEN_ID));

    // When
    rollbackService.pidCreationFailed(List.of(givenDigitalSpecimenEvent()));

    // Then
    then(rabbitMQService).should().deadLetterEvent(givenDigitalSpecimenEvent());
  }

  @Test
  void testPidCreationFailedRollbackFailedDlqFailed() throws Exception {
    // Given
    doThrow(PidException.class).when(handleComponent)
        .rollbackFromPhysId(List.of(PHYSICAL_SPECIMEN_ID));
    doThrow(JsonProcessingException.class).when(rabbitMQService)
        .deadLetterEvent(givenDigitalSpecimenEvent());

    // When / Then
    assertDoesNotThrow(
        () -> rollbackService.pidCreationFailed(List.of(givenDigitalSpecimenEvent())));
  }


}
