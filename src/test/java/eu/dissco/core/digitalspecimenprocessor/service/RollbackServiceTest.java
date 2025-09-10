package eu.dissco.core.digitalspecimenprocessor.service;

import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.ANOTHER_SPECIMEN_NAME;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.HANDLE;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MAS;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MEDIA_PID;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MEDIA_PID_ALT;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MEDIA_URL_ALT;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.ORGANISATION_ID;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.PHYSICAL_SPECIMEN_ID_ALT;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.SECOND_HANDLE;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.VERSION;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalMediaEventWithSpecimenEr;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalMediaRecord;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenEvent;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenRecord;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenEmptyMediaProcessResult;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenJsonPatchSpecimen;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenUnequalDigitalMediaEvent;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenUnequalDigitalMediaRecord;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenUnequalDigitalSpecimenRecord;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenUpdatedDigitalMediaRecord;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenUpdatedDigitalSpecimenRecord;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;

import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch._types.ErrorCause;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import com.fasterxml.jackson.core.JsonProcessingException;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.UpdatedDigitalSpecimenRecord;
import eu.dissco.core.digitalspecimenprocessor.exception.PidException;
import eu.dissco.core.digitalspecimenprocessor.repository.DigitalMediaRepository;
import eu.dissco.core.digitalspecimenprocessor.repository.DigitalSpecimenRepository;
import eu.dissco.core.digitalspecimenprocessor.repository.ElasticSearchRepository;
import eu.dissco.core.digitalspecimenprocessor.web.HandleComponent;
import java.util.List;
import java.util.Set;
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
  RabbitMqPublisherService rabbitMqService;
  @Mock
  DigitalSpecimenRepository specimenRepository;
  @Mock
  DigitalMediaRepository mediaRepository;
  @Mock
  FdoRecordService fdoRecordService;
  @Mock
  HandleComponent handleComponent;
  @Mock
  private BulkResponse bulkResponse;
  private RollbackService rollbackService;



  @BeforeEach
  void init() {
    rollbackService = new RollbackService(elasticSearchRepository, rabbitMqService,
        specimenRepository, mediaRepository, fdoRecordService, handleComponent);
  }

  // Naming convention
  // Case 1: Republish event only
  // Case 2: Republish event, rollback database
  // Case 3: Republish event, rollback database, rollback elastic


  @Test
  void testRollbackNewSpecimensCase1() throws Exception {
    // Given

    // When
    rollbackService.rollbackNewSpecimens(Set.of(givenDigitalSpecimenRecord()), false,
        false);

    // Then
    then(elasticSearchRepository).shouldHaveNoInteractions();
    then(specimenRepository).shouldHaveNoInteractions();
    then(rabbitMqService).should(times(1)).deadLetterEventSpecimen(any());
  }

  @Test
  void testRollbackNewSpecimensCase2() throws Exception {
    // Given

    // When
    rollbackService.rollbackNewSpecimens(Set.of(givenDigitalSpecimenRecord()), false, true);

    // Then
    then(elasticSearchRepository).shouldHaveNoInteractions();
    then(specimenRepository).should().rollbackSpecimen(HANDLE);
    then(rabbitMqService).should(times(1)).deadLetterEventSpecimen(any());
  }

  @Test
  void testRollbackNewSpecimensCase3() throws Exception {
    // Given

    // When
    rollbackService.rollbackNewSpecimens(Set.of(givenDigitalSpecimenRecord()), true, true);

    // Then
    then(elasticSearchRepository).should().rollbackObject(HANDLE, true);
    then(specimenRepository).should().rollbackSpecimen(HANDLE);
    then(rabbitMqService).should(times(1)).deadLetterEventSpecimen(any());
  }

  @Test
  void testRollbackNewSpecimensCase3ElasticFails() throws Exception {
    // Given
    doThrow(ElasticsearchException.class).when(elasticSearchRepository)
        .rollbackObject(HANDLE, true);

    // When
    rollbackService.rollbackNewSpecimens(Set.of(givenDigitalSpecimenRecord()), true, true);

    // Then
    then(specimenRepository).should().rollbackSpecimen(HANDLE);
    then(rabbitMqService).should(times(1)).deadLetterEventSpecimen(any());
  }

  @Test
  void testRollbackNewSpecimensCase3DlqFails() throws Exception {
    // Given
    doThrow(JsonProcessingException.class).when(rabbitMqService).deadLetterEventSpecimen(any());

    // When
    rollbackService.rollbackNewSpecimens(Set.of(givenDigitalSpecimenRecord()), true, true);

    // Then
    then(specimenRepository).should().rollbackSpecimen(HANDLE);
    then(rabbitMqService).should(times(1)).deadLetterEventSpecimen(any());
  }

  @Test
  void testRollbackNewMediaCase1() throws Exception {
    // Given

    // When
    rollbackService.rollbackNewMedias(Set.of(givenDigitalMediaRecord()), false, false);

    // Then
    then(mediaRepository).shouldHaveNoInteractions();
    then(elasticSearchRepository).shouldHaveNoInteractions();
    then(rabbitMqService).should().deadLetterEventMedia(givenDigitalMediaEventWithSpecimenEr());
  }

  @Test
  void testRollbackNewMediaCase2() throws Exception {
    // Given

    // When
    rollbackService.rollbackNewMedias(Set.of(givenDigitalMediaRecord()), false, true);

    // Then
    then(mediaRepository).should().rollBackDigitalMedia(MEDIA_PID);
    then(elasticSearchRepository).shouldHaveNoInteractions();
    then(rabbitMqService).should().deadLetterEventMedia(givenDigitalMediaEventWithSpecimenEr());
  }

  @Test
  void testRollbackNewMediaCase3() throws Exception {
    // Given

    // When
    rollbackService.rollbackNewMedias(Set.of(givenDigitalMediaRecord()), true, true);

    // Then
    then(mediaRepository).should().rollBackDigitalMedia(MEDIA_PID);
    then(elasticSearchRepository).should().rollbackObject(MEDIA_PID, false);
    then(rabbitMqService).should().deadLetterEventMedia(givenDigitalMediaEventWithSpecimenEr());
  }

  @Test
  void testRollbackNewMediaCase3ElasticFails() throws Exception {
    // Given
    doThrow(ElasticsearchException.class).when(elasticSearchRepository)
        .rollbackObject(MEDIA_PID, false);

    // When
    rollbackService.rollbackNewMedias(Set.of(givenDigitalMediaRecord()), true, true);

    // Then
    then(mediaRepository).should().rollBackDigitalMedia(MEDIA_PID);
    then(rabbitMqService).should().deadLetterEventMedia(givenDigitalMediaEventWithSpecimenEr());
  }

  @Test
  void testRollbackNewMediaCase3DlqFails() throws Exception {
    // Given
    doThrow(JsonProcessingException.class).when(rabbitMqService).deadLetterEventMedia(any());

    // When
    rollbackService.rollbackNewMedias(Set.of(givenDigitalMediaRecord()), true, true);

    // Then
    then(mediaRepository).should().rollBackDigitalMedia(MEDIA_PID);
    then(rabbitMqService).should().deadLetterEventMedia(givenDigitalMediaEventWithSpecimenEr());
  }

  @Test
  void rollbackUpdatedSpecimenCase1() throws Exception {
    // Given
    var specimenRecords = Set.of(givenUpdatedDigitalSpecimenRecord(false));
    given(fdoRecordService.handleNeedsUpdateSpecimen(any(), any())).willReturn(false);

    // When
    rollbackService.rollbackUpdatedSpecimens(specimenRecords, false, false);

    // Then
    then(elasticSearchRepository).shouldHaveNoInteractions();
    then(specimenRepository).shouldHaveNoInteractions();
    then(handleComponent).shouldHaveNoInteractions();
    then(rabbitMqService).should(times(1)).deadLetterEventSpecimen(any());
  }

  @Test
  void rollbackUpdatedSpecimenCase2() throws Exception {
    // Given
    var specimenRecords = Set.of(givenUpdatedDigitalSpecimenRecord(false));
    given(fdoRecordService.handleNeedsUpdateSpecimen(any(), any())).willReturn(false);

    // When
    rollbackService.rollbackUpdatedSpecimens(specimenRecords, false, true);

    // Then
    then(elasticSearchRepository).shouldHaveNoInteractions();
    then(specimenRepository).should()
        .createDigitalSpecimenRecord(Set.of(givenUnequalDigitalSpecimenRecord()));
    then(handleComponent).shouldHaveNoInteractions();
    then(rabbitMqService).should(times(1)).deadLetterEventSpecimen(any());
  }

  @Test
  void rollbackUpdatedSpecimenCase3() throws Exception {
    // Given
    var specimenRecords = Set.of(givenUpdatedDigitalSpecimenRecord(false));
    given(fdoRecordService.handleNeedsUpdateSpecimen(any(), any())).willReturn(false);

    // When
    rollbackService.rollbackUpdatedSpecimens(specimenRecords, true, true);

    // Then
    then(elasticSearchRepository).should().rollbackVersion(givenUnequalDigitalSpecimenRecord());
    then(specimenRepository).should()
        .createDigitalSpecimenRecord(Set.of(givenUnequalDigitalSpecimenRecord()));
    then(handleComponent).shouldHaveNoInteractions();
    then(rabbitMqService).should(times(1)).deadLetterEventSpecimen(any());
  }

  @Test
  void rollbackUpdatedSpecimenCase3HandleNeedsUpdate() throws Exception {
    // Given
    var specimenRecords = Set.of(givenUpdatedDigitalSpecimenRecord(false));
    given(fdoRecordService.handleNeedsUpdateSpecimen(any(), any())).willReturn(true);

    // When
    rollbackService.rollbackUpdatedSpecimens(specimenRecords, true, true);

    // Then
    then(elasticSearchRepository).should().rollbackVersion(givenUnequalDigitalSpecimenRecord());
    then(specimenRepository).should()
        .createDigitalSpecimenRecord(Set.of(givenUnequalDigitalSpecimenRecord()));
    then(handleComponent).should().rollbackHandleUpdate(any());
    then(rabbitMqService).should(times(1)).deadLetterEventSpecimen(any());
  }

  @Test
  void rollbackUpdatedSpecimenCase3HandleNeedsUpdateHandleFails() throws Exception {
    // Given
    var specimenRecords = Set.of(givenUpdatedDigitalSpecimenRecord(false));
    given(fdoRecordService.handleNeedsUpdateSpecimen(any(), any())).willReturn(true);
    doThrow(PidException.class).when(handleComponent).rollbackHandleUpdate(any());

    // When
    rollbackService.rollbackUpdatedSpecimens(specimenRecords, true, true);

    // Then
    then(elasticSearchRepository).should().rollbackVersion(givenUnequalDigitalSpecimenRecord());
    then(specimenRepository).should()
        .createDigitalSpecimenRecord(Set.of(givenUnequalDigitalSpecimenRecord()));
    then(rabbitMqService).should(times(1)).deadLetterEventSpecimen(any());
  }

  @Test
  void rollbackUpdatedSpecimenCase3ElasticFails() throws Exception {
    // Given
    var specimenRecords = Set.of(givenUpdatedDigitalSpecimenRecord(false));
    given(fdoRecordService.handleNeedsUpdateSpecimen(any(), any())).willReturn(false);
    doThrow(ElasticsearchException.class).when(elasticSearchRepository)
        .rollbackVersion(givenUnequalDigitalSpecimenRecord());

    // When
    rollbackService.rollbackUpdatedSpecimens(specimenRecords, true, true);

    // Then
    then(specimenRepository).should()
        .createDigitalSpecimenRecord(Set.of(givenUnequalDigitalSpecimenRecord()));
    then(handleComponent).shouldHaveNoInteractions();
    then(rabbitMqService).should(times(1)).deadLetterEventSpecimen(any());
  }

  @Test
  void rollbackUpdatedSpecimenCase3DlqFails() throws Exception {
    // Given
    var specimenRecords = Set.of(givenUpdatedDigitalSpecimenRecord(false));
    given(fdoRecordService.handleNeedsUpdateSpecimen(any(), any())).willReturn(false);
    doThrow(JsonProcessingException.class).when(rabbitMqService).deadLetterEventSpecimen(any());

    // When
    rollbackService.rollbackUpdatedSpecimens(specimenRecords, true, true);

    // Then
    then(elasticSearchRepository).should().rollbackVersion(givenUnequalDigitalSpecimenRecord());
    then(specimenRepository).should()
        .createDigitalSpecimenRecord(Set.of(givenUnequalDigitalSpecimenRecord()));
    then(handleComponent).shouldHaveNoInteractions();
    then(rabbitMqService).should(times(1)).deadLetterEventSpecimen(any());
  }

  @Test
  void testRollbackUpdatedMediaCase1() throws Exception {
    // Given
    given(fdoRecordService.handleNeedsUpdateMedia(any(), any())).willReturn(false);

    // When
    rollbackService.rollbackUpdatedMedias(
        Set.of(givenUpdatedDigitalMediaRecord()), false, false);

    // Then
    then(mediaRepository).shouldHaveNoInteractions();
    then(elasticSearchRepository).shouldHaveNoInteractions();
    then(handleComponent).shouldHaveNoInteractions();
  }

  @Test
  void testRollbackUpdatedMediaCase2() throws Exception {
    // Given
    given(fdoRecordService.handleNeedsUpdateMedia(any(), any())).willReturn(false);

    // When
    rollbackService.rollbackUpdatedMedias(
        Set.of(givenUpdatedDigitalMediaRecord()), false, true);

    // Then
    then(mediaRepository).should().createDigitalMediaRecord(Set.of(givenDigitalMediaRecord()));
    then(elasticSearchRepository).shouldHaveNoInteractions();
    then(handleComponent).shouldHaveNoInteractions();
  }

  @Test
  void testRollbackUpdatedMediaCase3() throws Exception {
    // Given
    given(fdoRecordService.handleNeedsUpdateMedia(any(), any())).willReturn(false);

    // When
    rollbackService.rollbackUpdatedMedias(
        Set.of(givenUpdatedDigitalMediaRecord()), true, true);

    // Then
    then(mediaRepository).should().createDigitalMediaRecord(Set.of(givenDigitalMediaRecord()));
    then(elasticSearchRepository).should().rollbackVersion(givenDigitalMediaRecord());
    then(handleComponent).shouldHaveNoInteractions();
  }

  @Test
  void testRollbackUpdatedMediaCase3ElasticFails() throws Exception {
    // Given
    given(fdoRecordService.handleNeedsUpdateMedia(any(), any())).willReturn(false);
    doThrow(ElasticsearchException.class).when(elasticSearchRepository).rollbackVersion(givenDigitalMediaRecord());

    // When
    rollbackService.rollbackUpdatedMedias(
        Set.of(givenUpdatedDigitalMediaRecord()), true, true);

    // Then
    then(mediaRepository).should().createDigitalMediaRecord(Set.of(givenDigitalMediaRecord()));
    then(handleComponent).shouldHaveNoInteractions();
  }

  @Test
  void testRollbackUpdatedMediaCase3DlqFailed() throws Exception {
    // Given
    given(fdoRecordService.handleNeedsUpdateMedia(any(), any())).willReturn(false);
    doThrow(JsonProcessingException.class).when(rabbitMqService).deadLetterEventMedia(any());

    // When
    rollbackService.rollbackUpdatedMedias(
        Set.of(givenUpdatedDigitalMediaRecord()), true, true);

    // Then
    then(mediaRepository).should().createDigitalMediaRecord(Set.of(givenDigitalMediaRecord()));
    then(elasticSearchRepository).should().rollbackVersion(givenDigitalMediaRecord());
    then(handleComponent).shouldHaveNoInteractions();
  }

  @Test
  void testRollbackUpdatedMediaCase3HandleNeedsUpdate() throws Exception {
    // Given
    given(fdoRecordService.handleNeedsUpdateMedia(any(), any())).willReturn(true);

    // When
    rollbackService.rollbackUpdatedMedias(
        Set.of(givenUpdatedDigitalMediaRecord()), true, true);

    // Then
    then(mediaRepository).should().createDigitalMediaRecord(Set.of(givenDigitalMediaRecord()));
    then(elasticSearchRepository).should().rollbackVersion(givenDigitalMediaRecord());
    then(handleComponent).should().rollbackHandleUpdate(any());
  }

  @Test
  void testHandlePartiallyFailedInsertSpecimen() throws Exception {
    // Given
    var successfulRecord = givenDigitalSpecimenRecord();
    var failedRecord = givenDigitalSpecimenRecord(SECOND_HANDLE, PHYSICAL_SPECIMEN_ID_ALT, true);
    var failedEvent = givenDigitalSpecimenEvent(PHYSICAL_SPECIMEN_ID_ALT);
    givenBulkResponse(HANDLE, SECOND_HANDLE);

    // When
    var result = rollbackService.handlePartiallyFailedElasticInsertSpecimen(Set.of(successfulRecord, failedRecord),
        bulkResponse);

    // Then
    then(rabbitMqService).should().publishCreateEventSpecimen(successfulRecord);
    then(rabbitMqService).should().deadLetterEventSpecimen(failedEvent);
    then(specimenRepository).should().rollbackSpecimen(SECOND_HANDLE);
    then(elasticSearchRepository).shouldHaveNoMoreInteractions();
    assertThat(result).isEqualTo(Set.of(successfulRecord));
  }

  @Test
  void testHandlePartiallyFailedInsertSpecimenPubFails() throws Exception {
    // Given
    var successfulRecord = givenDigitalSpecimenRecord();
    var failedRecord = givenDigitalSpecimenRecord(SECOND_HANDLE, PHYSICAL_SPECIMEN_ID_ALT, false);
    givenBulkResponse(HANDLE, SECOND_HANDLE);
    doThrow(JsonProcessingException.class).when(rabbitMqService).publishCreateEventSpecimen(any());

    // When
    var result = rollbackService.handlePartiallyFailedElasticInsertSpecimen(Set.of(successfulRecord, failedRecord),
        bulkResponse);

    // Then
    assertThat(result).isEmpty();
    then(rabbitMqService).should(times(2)).deadLetterEventSpecimen(any());
    then(specimenRepository).should(times(2)).rollbackSpecimen(any());
    then(elasticSearchRepository).should().rollbackObject(HANDLE, true);
  }

  @Test
  void testHandlePartiallyFailedInsertMedia() throws Exception {
    // Given
    var successfulRecord = givenDigitalMediaRecord();
    var failedRecord = givenUnequalDigitalMediaRecord(MEDIA_PID_ALT, MEDIA_URL_ALT, VERSION);
    var records = Set.of(successfulRecord, failedRecord);

    var failedEvent = givenUnequalDigitalMediaEvent(MEDIA_URL_ALT, true);
    givenBulkResponse(MEDIA_PID, MEDIA_PID_ALT);

    // When
    var result = rollbackService.handlePartiallyFailedElasticInsertMedia(records,
        bulkResponse);

    // Then
    then(rabbitMqService).should().deadLetterEventMedia(failedEvent);
    then(rabbitMqService).should().publishCreateEventMedia(successfulRecord);
    then(mediaRepository).should().rollBackDigitalMedia(MEDIA_PID_ALT);
    then(elasticSearchRepository).shouldHaveNoInteractions();
    assertThat(result).isEqualTo(Set.of(successfulRecord));
  }

  @Test
  void testHandlePartiallyFailedInsertMediaPubFails() throws Exception {
    // Given
    var successfulRecord = givenDigitalMediaRecord();
    var failedRecord = givenUnequalDigitalMediaRecord(MEDIA_PID_ALT, MEDIA_URL_ALT, VERSION);
    var records = Set.of(successfulRecord, failedRecord);

    givenBulkResponse(MEDIA_PID, MEDIA_PID_ALT);
    doThrow(JsonProcessingException.class).when(rabbitMqService).publishCreateEventMedia(any());

    // When
    var result = rollbackService.handlePartiallyFailedElasticInsertMedia(records,
        bulkResponse);

    // Then
    then(rabbitMqService).should(times(2)).deadLetterEventMedia(any());
    then(mediaRepository).should(times(2)).rollBackDigitalMedia(any());
    then(elasticSearchRepository).should().rollbackObject(MEDIA_PID, false);
    assertThat(result).isEmpty();
  }


  private void givenBulkResponse(String successfulPid, String failedPid) {
    var positiveResponse = mock(BulkResponseItem.class);
    given(positiveResponse.error()).willReturn(null);
    given(positiveResponse.id()).willReturn(successfulPid);
    var negativeResponse = mock(BulkResponseItem.class);
    given(negativeResponse.error()).willReturn(new ErrorCause.Builder().reason("Crashed").build());
    given(negativeResponse.id()).willReturn(failedPid);
    given(bulkResponse.items()).willReturn(
        List.of(positiveResponse, negativeResponse));
  }

  @Test
  void testHandlePartiallyFailedElasticUpdateSpecimen() throws Exception {
    // Given
    var successfulRecord = givenUpdatedDigitalSpecimenRecord(false);
    var failedRecord = new UpdatedDigitalSpecimenRecord(
        givenUnequalDigitalSpecimenRecord(SECOND_HANDLE, ANOTHER_SPECIMEN_NAME, ORGANISATION_ID, false, PHYSICAL_SPECIMEN_ID_ALT),
        Set.of(MAS),
        givenDigitalSpecimenRecord(SECOND_HANDLE, PHYSICAL_SPECIMEN_ID_ALT, false),
        givenJsonPatchSpecimen(),
        List.of(),
        givenEmptyMediaProcessResult());
    givenBulkResponse(HANDLE, SECOND_HANDLE);
    given(fdoRecordService.handleNeedsUpdateSpecimen(any(), any())).willReturn(true);

    // When
    var result = rollbackService.handlePartiallyFailedElasticUpdateSpecimen(Set.of(successfulRecord, failedRecord),
        bulkResponse);

    // Then
    assertThat(result).isEqualTo(Set.of(successfulRecord));
    then(rabbitMqService).should(times(1)).publishUpdateEventSpecimen(any(), any());
    then(rabbitMqService).should(times(1)).deadLetterEventSpecimen(any());
    then(rabbitMqService).shouldHaveNoMoreInteractions();
    then(elasticSearchRepository).shouldHaveNoInteractions();
    then(specimenRepository).should().createDigitalSpecimenRecord(Set.of(failedRecord.currentDigitalSpecimen()));
    then(handleComponent).should().rollbackHandleUpdate(any());
  }

  @Test
  void testHandlePartiallyFailedElasticUpdateSpecimenPublishingFails() throws Exception {
    // Given
    var successfulRecord = givenUpdatedDigitalSpecimenRecord(false);
    var failedRecord = new UpdatedDigitalSpecimenRecord(
        givenUnequalDigitalSpecimenRecord(SECOND_HANDLE, ANOTHER_SPECIMEN_NAME, ORGANISATION_ID, false, PHYSICAL_SPECIMEN_ID_ALT),
        Set.of(MAS),
        givenDigitalSpecimenRecord(SECOND_HANDLE, PHYSICAL_SPECIMEN_ID_ALT, false),
        givenJsonPatchSpecimen(),
        List.of(),
        givenEmptyMediaProcessResult());
    givenBulkResponse(HANDLE, SECOND_HANDLE);
    given(fdoRecordService.handleNeedsUpdateSpecimen(any(), any())).willReturn(false);
    doThrow(JsonProcessingException.class).when(rabbitMqService).publishUpdateEventSpecimen(any(), any());

    // When
    var result = rollbackService.handlePartiallyFailedElasticUpdateSpecimen(Set.of(successfulRecord, failedRecord),
        bulkResponse);

    // Then
    assertThat(result).isEmpty();
    then(rabbitMqService).should(times(2)).deadLetterEventSpecimen(any());
    then(rabbitMqService).shouldHaveNoMoreInteractions();
    then(specimenRepository).should(times(2)).createDigitalSpecimenRecord(any());
    then(elasticSearchRepository).should().rollbackVersion(successfulRecord.currentDigitalSpecimen());
    then(handleComponent).shouldHaveNoInteractions();
  }

  @Test
  void testHandlePartiallyFailedElasticUpdateMedia() throws Exception {
    // Given
    var successfulRecord = givenUpdatedDigitalMediaRecord();
    var failedRecord = givenUpdatedDigitalMediaRecord(MEDIA_PID_ALT, MEDIA_URL_ALT);
    givenBulkResponse(MEDIA_PID, MEDIA_PID_ALT);
    given(fdoRecordService.handleNeedsUpdateMedia(any(), any())).willReturn(true);

    // When
    var result = rollbackService.handlePartiallyFailedElasticUpdateMedia(Set.of(successfulRecord, failedRecord),
        bulkResponse);

    // Then
    assertThat(result).isEqualTo(Set.of(successfulRecord));
    then(rabbitMqService).should(times(1)).publishUpdateEventMedia(any(), any());
    then(rabbitMqService).should(times(1)).deadLetterEventMedia(any());
    then(rabbitMqService).shouldHaveNoMoreInteractions();
    then(elasticSearchRepository).shouldHaveNoInteractions();
    then(mediaRepository).should().createDigitalMediaRecord(Set.of(failedRecord.currentDigitalMediaRecord()));
    then(handleComponent).should().rollbackHandleUpdate(any());
  }

  @Test
  void testHandlePartiallyFailedElasticUpdateMediaPubFails() throws Exception {
    // Given
    var successfulRecord = givenUpdatedDigitalMediaRecord();
    var failedRecord = givenUpdatedDigitalMediaRecord(MEDIA_PID_ALT, MEDIA_URL_ALT);
    givenBulkResponse(MEDIA_PID, MEDIA_PID_ALT);
    given(fdoRecordService.handleNeedsUpdateMedia(any(), any())).willReturn(true);
    doThrow(JsonProcessingException.class).when(rabbitMqService).publishUpdateEventMedia(any(), any());

    // When
    var result = rollbackService.handlePartiallyFailedElasticUpdateMedia(Set.of(successfulRecord, failedRecord),
        bulkResponse);

    // Then
    assertThat(result).isEmpty();
    then(rabbitMqService).should(times(2)).deadLetterEventMedia(any());
    then(elasticSearchRepository).should().rollbackVersion(successfulRecord.currentDigitalMediaRecord());
    then(mediaRepository).should(times(2)).createDigitalMediaRecord(any());
    then(handleComponent).should().rollbackHandleUpdate(any());
  }

}
