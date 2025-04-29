package eu.dissco.core.digitalspecimenprocessor.service;

import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.HANDLE;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MEDIA_PID;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MEDIA_URL;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MEDIA_URL_ALT;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.PHYSICAL_SPECIMEN_ID;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalMediaEvent;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalMediaEventWithSpecimenEr;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalMediaRecord;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenEvent;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenRecord;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenPidProcessResultMedia;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenPidProcessResultSpecimen;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenUnequalDigitalSpecimenRecord;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenUpdatedDigitalMediaRecord;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenUpdatedDigitalSpecimenRecord;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;

import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.nimbusds.jose.util.Pair;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaEvent;
import eu.dissco.core.digitalspecimenprocessor.exception.PidException;
import eu.dissco.core.digitalspecimenprocessor.repository.DigitalMediaRepository;
import eu.dissco.core.digitalspecimenprocessor.repository.DigitalSpecimenRepository;
import eu.dissco.core.digitalspecimenprocessor.repository.ElasticSearchRepository;
import eu.dissco.core.digitalspecimenprocessor.web.HandleComponent;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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

  // Naming convesion
  // Case 1: Rollback handles only
  // Case 2: Rollback handles, database
  // Case 3: Rollback handles, database, elastic


  @Test
  void testRollbackNewSpecimensCase1() throws Exception {
    // Given
    var pidMap = Map.of(PHYSICAL_SPECIMEN_ID, givenPidProcessResultSpecimen(false));

    // When
    rollbackService.rollbackNewSpecimens(List.of(givenDigitalSpecimenEvent()), pidMap, false,
        false);

    // Then
    then(elasticSearchRepository).shouldHaveNoInteractions();
    then(specimenRepository).shouldHaveNoInteractions();
    then(rabbitMqService).should(times(1)).deadLetterEventSpecimen(any());
    then(handleComponent).should(times(1)).rollbackHandleCreation(Set.of(HANDLE));
  }

  @Test
  void testRollbackNewSpecimensCase2() throws Exception {
    // Given
    var pidMap = Map.of(PHYSICAL_SPECIMEN_ID, givenPidProcessResultSpecimen(false));

    // When
    rollbackService.rollbackNewSpecimens(List.of(givenDigitalSpecimenEvent()), pidMap, false, true);

    // Then
    then(elasticSearchRepository).shouldHaveNoInteractions();
    then(specimenRepository).should().rollbackSpecimen(HANDLE);
    then(rabbitMqService).should(times(1)).deadLetterEventSpecimen(any());
    then(handleComponent).should(times(1)).rollbackHandleCreation(Set.of(HANDLE));
  }

  @Test
  void testRollbackNewSpecimensCase3() throws Exception {
    // Given
    var pidMap = Map.of(PHYSICAL_SPECIMEN_ID, givenPidProcessResultSpecimen(false));

    // When
    rollbackService.rollbackNewSpecimens(List.of(givenDigitalSpecimenEvent()), pidMap, true, true);

    // Then
    then(elasticSearchRepository).should().rollbackObject(HANDLE, true);
    then(specimenRepository).should().rollbackSpecimen(HANDLE);
    then(rabbitMqService).should(times(1)).deadLetterEventSpecimen(any());
    then(handleComponent).should(times(1)).rollbackHandleCreation(Set.of(HANDLE));
  }

  @Test
  void testRollbackNewSpecimensCase3ElasticFails() throws Exception {
    // Given
    var pidMap = Map.of(PHYSICAL_SPECIMEN_ID, givenPidProcessResultSpecimen(false));
    doThrow(ElasticsearchException.class).when(elasticSearchRepository)
        .rollbackObject(HANDLE, true);

    // When
    rollbackService.rollbackNewSpecimens(List.of(givenDigitalSpecimenEvent()), pidMap, true, true);

    // Then
    then(specimenRepository).should().rollbackSpecimen(HANDLE);
    then(rabbitMqService).should(times(1)).deadLetterEventSpecimen(any());
    then(handleComponent).should(times(1)).rollbackHandleCreation(Set.of(HANDLE));
  }

  @Test
  void testRollbackNewSpecimensCase3DlqFails() throws Exception {
    // Given
    var pidMap = Map.of(PHYSICAL_SPECIMEN_ID, givenPidProcessResultSpecimen(false));
    doThrow(JsonProcessingException.class).when(rabbitMqService).deadLetterEventSpecimen(any());

    // When
    rollbackService.rollbackNewSpecimens(List.of(givenDigitalSpecimenEvent()), pidMap, true, true);

    // Then
    then(specimenRepository).should().rollbackSpecimen(HANDLE);
    then(rabbitMqService).should(times(1)).deadLetterEventSpecimen(any());
    then(handleComponent).should(times(1)).rollbackHandleCreation(Set.of(HANDLE));
  }

  @Test
  void testRollbackNewMediaCase1() throws Exception {
    // Given
    var pidMap = Map.of(MEDIA_URL, givenPidProcessResultMedia());

    // When
    rollbackService.rollbackNewMedias(List.of(givenDigitalMediaEvent()), pidMap, false, false);

    // Then
    then(mediaRepository).shouldHaveNoInteractions();
    then(elasticSearchRepository).shouldHaveNoInteractions();
    then(rabbitMqService).should().deadLetterEventMedia(givenDigitalMediaEvent());
    then(handleComponent).should().rollbackHandleCreation(Set.of(MEDIA_PID));
  }

  @Test
  void testRollbackNewMediaCase2() throws Exception {
    // Given
    var pidMap = Map.of(MEDIA_URL, givenPidProcessResultMedia());

    // When
    rollbackService.rollbackNewMedias(List.of(givenDigitalMediaEvent()), pidMap, false, true);

    // Then
    then(mediaRepository).should().rollBackDigitalMedia(MEDIA_PID);
    then(elasticSearchRepository).shouldHaveNoInteractions();
    then(rabbitMqService).should().deadLetterEventMedia(givenDigitalMediaEvent());
    then(handleComponent).should().rollbackHandleCreation(Set.of(MEDIA_PID));
  }

  @Test
  void testRollbackNewMediaCase3() throws Exception {
    // Given
    var pidMap = Map.of(MEDIA_URL, givenPidProcessResultMedia());

    // When
    rollbackService.rollbackNewMedias(List.of(givenDigitalMediaEvent()), pidMap, true, true);

    // Then
    then(mediaRepository).should().rollBackDigitalMedia(MEDIA_PID);
    then(elasticSearchRepository).should().rollbackObject(MEDIA_PID, false);
    then(rabbitMqService).should().deadLetterEventMedia(givenDigitalMediaEvent());
    then(handleComponent).should().rollbackHandleCreation(Set.of(MEDIA_PID));
  }

  @Test
  void testRollbackNewMediaCase3ElasticFails() throws Exception {
    // Given
    var pidMap = Map.of(MEDIA_URL, givenPidProcessResultMedia());
    doThrow(ElasticsearchException.class).when(elasticSearchRepository)
        .rollbackObject(MEDIA_PID, false);

    // When
    rollbackService.rollbackNewMedias(List.of(givenDigitalMediaEvent()), pidMap, true, true);

    // Then
    then(mediaRepository).should().rollBackDigitalMedia(MEDIA_PID);
    then(rabbitMqService).should().deadLetterEventMedia(givenDigitalMediaEvent());
    then(handleComponent).should().rollbackHandleCreation(Set.of(MEDIA_PID));
  }

  @Test
  void testRollbackNewMediaCase3DlqFails() throws Exception {
    // Given
    var pidMap = Map.of(MEDIA_URL, givenPidProcessResultMedia());
    doThrow(JsonProcessingException.class).when(rabbitMqService).deadLetterEventMedia(any());

    // When
    rollbackService.rollbackNewMedias(List.of(givenDigitalMediaEvent()), pidMap, true, true);

    // Then
    then(mediaRepository).should().rollBackDigitalMedia(MEDIA_PID);
    then(rabbitMqService).should().deadLetterEventMedia(givenDigitalMediaEvent());
    then(handleComponent).should().rollbackHandleCreation(Set.of(MEDIA_PID));
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
    var pidMap = Map.of(MEDIA_URL, givenPidProcessResultMedia());
    given(fdoRecordService.handleNeedsUpdateMedia(any(), any())).willReturn(false);

    // When
    rollbackService.rollbackUpdatedMedias(
        Set.of(givenUpdatedDigitalMediaRecord()), false, false, List.of(givenDigitalMediaEventWithSpecimenEr()), pidMap);

    // Then
    then(mediaRepository).shouldHaveNoInteractions();
    then(elasticSearchRepository).shouldHaveNoInteractions();
    then(handleComponent).shouldHaveNoInteractions();
  }

  @Test
  void testRollbackUpdatedMediaCase2() throws Exception {
    // Given
    var pidMap = Map.of(MEDIA_URL, givenPidProcessResultMedia());
    given(fdoRecordService.handleNeedsUpdateMedia(any(), any())).willReturn(false);

    // When
    rollbackService.rollbackUpdatedMedias(
        Set.of(givenUpdatedDigitalMediaRecord()), false, true, List.of(givenDigitalMediaEventWithSpecimenEr()), pidMap);

    // Then
    then(mediaRepository).should().createDigitalMediaRecord(Set.of(givenDigitalMediaRecord()));
    then(elasticSearchRepository).shouldHaveNoInteractions();
    then(handleComponent).shouldHaveNoInteractions();
  }

  @Test
  void testRollbackUpdatedMediaCase3() throws Exception {
    // Given
    var pidMap = Map.of(MEDIA_URL, givenPidProcessResultMedia());
    given(fdoRecordService.handleNeedsUpdateMedia(any(), any())).willReturn(false);

    // When
    rollbackService.rollbackUpdatedMedias(
        Set.of(givenUpdatedDigitalMediaRecord()), true, true, List.of(givenDigitalMediaEventWithSpecimenEr()), pidMap);

    // Then
    then(mediaRepository).should().createDigitalMediaRecord(Set.of(givenDigitalMediaRecord()));
    then(elasticSearchRepository).should().rollbackVersion(givenDigitalMediaRecord());
    then(handleComponent).shouldHaveNoInteractions();
  }

  @Test
  void testRollbackUpdatedMediaCase3ElasticFails() throws Exception {
    // Given
    var pidMap = Map.of(MEDIA_URL, givenPidProcessResultMedia());
    given(fdoRecordService.handleNeedsUpdateMedia(any(), any())).willReturn(false);
    doThrow(ElasticsearchException.class).when(elasticSearchRepository).rollbackVersion(givenDigitalMediaRecord());

    // When
    rollbackService.rollbackUpdatedMedias(
        Set.of(givenUpdatedDigitalMediaRecord()), true, true, List.of(givenDigitalMediaEventWithSpecimenEr()), pidMap);

    // Then
    then(mediaRepository).should().createDigitalMediaRecord(Set.of(givenDigitalMediaRecord()));
    then(handleComponent).shouldHaveNoInteractions();
  }

  @Test
  void testRollbackUpdatedMediaCase3DlqFailed() throws Exception {
    // Given
    var pidMap = Map.of(MEDIA_URL, givenPidProcessResultMedia());
    given(fdoRecordService.handleNeedsUpdateMedia(any(), any())).willReturn(false);
    doThrow(JsonProcessingException.class).when(rabbitMqService).deadLetterEventMedia(any());

    // When
    rollbackService.rollbackUpdatedMedias(
        Set.of(givenUpdatedDigitalMediaRecord()), true, true, List.of(givenDigitalMediaEventWithSpecimenEr()), pidMap);

    // Then
    then(mediaRepository).should().createDigitalMediaRecord(Set.of(givenDigitalMediaRecord()));
    then(elasticSearchRepository).should().rollbackVersion(givenDigitalMediaRecord());
    then(handleComponent).shouldHaveNoInteractions();
  }

  @Test
  void testRollbackUpdatedMediaCase3HandleNeedsUpdate() throws Exception {
    // Given
    var pidMap = Map.of(MEDIA_URL, givenPidProcessResultMedia());
    given(fdoRecordService.handleNeedsUpdateMedia(any(), any())).willReturn(true);

    // When
    rollbackService.rollbackUpdatedMedias(
        Set.of(givenUpdatedDigitalMediaRecord()), true, true, List.of(givenDigitalMediaEventWithSpecimenEr()), pidMap);

    // Then
    then(mediaRepository).should().createDigitalMediaRecord(Set.of(givenDigitalMediaRecord()));
    then(elasticSearchRepository).should().rollbackVersion(givenDigitalMediaRecord());
    then(handleComponent).should().rollbackHandleUpdate(any());
  }



  /*

  @Test
  void testRollbackNewSpecimensHasMediaCase1() throws Exception {
    // Given
    var specimenRecords = Map.of(givenDigitalSpecimenRecord(), MEDIA_EVENT_PAIR);

    // When
    rollbackService.rollbackNewSpecimens(specimenRecords, false, false);

    // Then
    then(elasticSearchRepository).shouldHaveNoInteractions();
    then(specimenRepository).shouldHaveNoInteractions();
    then(rabbitMqService).should(times(1)).deadLetterEventSpecimen(any());
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
    then(specimenRepository).should().rollbackSpecimen(HANDLE);
    then(rabbitMqService).should(times(1)).deadLetterEventSpecimen(any());
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
    then(specimenRepository).should().rollbackSpecimen(HANDLE);
    then(rabbitMqService).should(times(1)).deadLetterEventSpecimen(any());
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
    then(specimenRepository).should().rollbackSpecimen(HANDLE);
    then(rabbitMqService).should(times(1)).deadLetterEventSpecimen(any());
    then(handleComponent).should(times(1)).rollbackHandleCreation(List.of(HANDLE));
    then(handleComponent).should(times(1)).rollbackHandleCreation(List.of(MEDIA_PID));
  }

  @Test
  void testRollbackNewSpecimensHasMediaCase3KafkaFailed() throws Exception {
    // Given
    var specimenRecords = Map.of(givenDigitalSpecimenRecord(), MEDIA_EVENT_PAIR);
    doThrow(JsonProcessingException.class).when(rabbitMqService).deadLetterEventSpecimen(any());

    // When
    rollbackService.rollbackNewSpecimens(specimenRecords, true, true);

    // Then
    then(elasticSearchRepository).should().rollbackSpecimen(givenDigitalSpecimenRecord());
    then(specimenRepository).should().rollbackSpecimen(HANDLE);
    then(rabbitMqService).should(times(1)).deadLetterEventSpecimen(any());
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
    then(specimenRepository).should().rollbackSpecimen(HANDLE);
    then(rabbitMqService).should(times(1)).deadLetterEventSpecimen(any());
    then(handleComponent).should(times(1)).rollbackHandleCreation(List.of(HANDLE));
    then(handleComponent).should(times(1)).rollbackHandleCreation(List.of(MEDIA_PID));
  }

  // Rollback Updated Specimen
  @Test
  void rollbackUpdatedSpecimenNoMediaNoUpdateHandleCase1() throws Exception {
    // Given
    var specimenRecords = List.of(givenUpdatedDigitalSpecimenRecord(false));
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
  void rollbackUpdatedSpecimenNoMediaCase1() throws Exception {
    // Given
    var specimenRecords = List.of(givenUpdatedDigitalSpecimenRecord(false));
    given(fdoRecordService.handleNeedsUpdateSpecimen(any(), any())).willReturn(true);

    // When
    rollbackService.rollbackUpdatedSpecimens(specimenRecords, false, false);

    // Then
    then(elasticSearchRepository).shouldHaveNoInteractions();
    then(specimenRepository).shouldHaveNoInteractions();
    then(handleComponent).should().rollbackHandleUpdate(anyList());
    then(rabbitMqService).should(times(1)).deadLetterEventSpecimen(any());
  }

  @Test
  void rollbackUpdatedSpecimenNoMediaCase2() throws Exception {
    // Given
    var specimenRecords = List.of(givenUpdatedDigitalSpecimenRecord(false));
    given(fdoRecordService.handleNeedsUpdateSpecimen(any(), any())).willReturn(true);

    // When
    rollbackService.rollbackUpdatedSpecimens(specimenRecords, false, true);

    // Then
    then(elasticSearchRepository).shouldHaveNoInteractions();
    then(specimenRepository).should().createDigitalSpecimenRecord(List.of(givenDigitalSpecimenRecord()));
    then(handleComponent).should().rollbackHandleUpdate(anyList());
    then(rabbitMqService).should(times(1)).deadLetterEventSpecimen(any());
  }

  @Test
  void rollbackUpdatedSpecimenNoMediaCase3() throws Exception {
    // Given
    var specimenRecords = List.of(givenUpdatedDigitalSpecimenRecord(false));
    var originalSpecimen = givenDigitalSpecimenRecord();
    given(fdoRecordService.handleNeedsUpdateSpecimen(any(), any())).willReturn(true);

    // When
    rollbackService.rollbackUpdatedSpecimens(specimenRecords, true, true);

    // Then
    then(elasticSearchRepository).should().rollbackVersion(originalSpecimen);
    then(specimenRepository).should().createDigitalSpecimenRecord(List.of(originalSpecimen));
    then(handleComponent).should().rollbackHandleUpdate(anyList());
    then(rabbitMqService).should(times(1)).deadLetterEventSpecimen(any());
  }


  @Test
  void rollbackUpdatedSpecimenHasMediaCase1() throws Exception {
    // Given
    var specimenRecords = List.of(givenUpdatedDigitalSpecimenRecord(false));
    given(fdoRecordService.handleNeedsUpdateSpecimen(any(), any())).willReturn(true);

    // When
    rollbackService.rollbackUpdatedSpecimens(specimenRecords, false,
        false);

    // Then
    then(elasticSearchRepository).shouldHaveNoInteractions();
    then(specimenRepository).shouldHaveNoInteractions();
    then(handleComponent).should().rollbackHandleUpdate(anyList());
    then(handleComponent).should(times(1)).rollbackHandleCreation(List.of(MEDIA_PID));
    then(rabbitMqService).should(times(1)).deadLetterEventSpecimen(any());
  }

  @Test
  void rollbackUpdatedSpecimenHasMediaCase2() throws Exception {
    // Given
    var specimenRecords = List.of(givenUpdatedDigitalSpecimenRecord(false));
    given(fdoRecordService.handleNeedsUpdateSpecimen(any(), any())).willReturn(true);

    // When
    rollbackService.rollbackUpdatedSpecimens(specimenRecords, false, true);

    // Then
    then(elasticSearchRepository).shouldHaveNoInteractions();
    then(specimenRepository).should().createDigitalSpecimenRecord(List.of(givenDigitalSpecimenRecord()));
    then(handleComponent).should().rollbackHandleUpdate(anyList());
    then(handleComponent).should(times(1)).rollbackHandleCreation(List.of(MEDIA_PID));
    then(rabbitMqService).should(times(1)).deadLetterEventSpecimen(any());
  }

  @Test
  void rollbackUpdatedSpecimenHasMediaCase3() throws Exception {
    // Given
    var specimenRecords = List.of(givenUpdatedDigitalSpecimenRecord(false));
    var originalSpecimen = givenDigitalSpecimenRecord();
    given(fdoRecordService.handleNeedsUpdateSpecimen(any(), any())).willReturn(true);

    // When
    rollbackService.rollbackUpdatedSpecimens(specimenRecords, true, true);

    // Then
    then(elasticSearchRepository).should().rollbackVersion(originalSpecimen);
    then(specimenRepository).should().createDigitalSpecimenRecord(List.of(originalSpecimen));
    then(handleComponent).should().rollbackHandleUpdate(anyList());
    then(handleComponent).should(times(1)).rollbackHandleCreation(List.of(MEDIA_PID));
    then(rabbitMqService).should(times(1)).deadLetterEventSpecimen(any());
  }

  @Test
  void rollbackUpdatedSpecimenHasMediaCase3ElasticFailed() throws Exception {
    // Given
    var specimenRecords = List.of(givenUpdatedDigitalSpecimenRecord(false));
    var originalSpecimen = givenDigitalSpecimenRecord();
    given(fdoRecordService.handleNeedsUpdateSpecimen(any(), any())).willReturn(true);
    doThrow(IOException.class).when(elasticSearchRepository)
        .rollbackVersion(givenDigitalSpecimenRecord());

    // When
    rollbackService.rollbackUpdatedSpecimens(specimenRecords, true, true);

    // Then
    then(elasticSearchRepository).should().rollbackVersion(originalSpecimen);
    then(specimenRepository).should().createDigitalSpecimenRecord(List.of(originalSpecimen));
    then(handleComponent).should().rollbackHandleUpdate(anyList());
    then(handleComponent).should(times(1)).rollbackHandleCreation(List.of(MEDIA_PID));
    then(rabbitMqService).should(times(1)).deadLetterEventSpecimen(any());
  }

  @Test
  void rollbackUpdatedSpecimenHasMediaCase3KafkaFailed() throws Exception {
    // Given
    var specimenRecords = List.of(givenUpdatedDigitalSpecimenRecord(false));
    var originalSpecimen = givenDigitalSpecimenRecord();
    given(fdoRecordService.handleNeedsUpdateSpecimen(any(), any())).willReturn(true);
    doThrow(JsonProcessingException.class).when(rabbitMqService).deadLetterEventSpecimen(any());

    // When
    rollbackService.rollbackUpdatedSpecimens(specimenRecords, true, true);

    // Then
    then(elasticSearchRepository).should().rollbackVersion(originalSpecimen);
    then(specimenRepository).should().createDigitalSpecimenRecord(List.of(originalSpecimen));
    then(handleComponent).should().rollbackHandleUpdate(anyList());
    then(handleComponent).should(times(1)).rollbackHandleCreation(List.of(MEDIA_PID));
    then(rabbitMqService).should(times(1)).deadLetterEventSpecimen(any());
  }

  @Test
  void rollbackUpdatedSpecimenHasMediaCase3PidFailed() throws Exception {
    // Given
    var specimenRecords = List.of(givenUpdatedDigitalSpecimenRecord(false));
    var originalSpecimen = givenDigitalSpecimenRecord();
    given(fdoRecordService.handleNeedsUpdateSpecimen(any(), any())).willReturn(true);
    doThrow(PidException.class).when(handleComponent).rollbackHandleUpdate(any());

    // When
    rollbackService.rollbackUpdatedSpecimens(specimenRecords, true, true);

    // Then
    then(elasticSearchRepository).should().rollbackVersion(originalSpecimen);
    then(specimenRepository).should().createDigitalSpecimenRecord(List.of(originalSpecimen));
    then(handleComponent).should().rollbackHandleUpdate(anyList());
    then(handleComponent).should(times(1)).rollbackHandleCreation(List.of(MEDIA_PID));
    then(rabbitMqService).should(times(1)).deadLetterEventSpecimen(any());
  }

  @Test
  void rollbackUpdatedSpecimenHasMediaCase3DatabaseFailed() throws Exception {
    // Given
    var specimenRecords = List.of(givenUpdatedDigitalSpecimenRecord(false));
    var originalSpecimen = givenDigitalSpecimenRecord();
    given(fdoRecordService.handleNeedsUpdateSpecimen(any(), any())).willReturn(true);
    doThrow(DataAccessException.class).when(specimenRepository).createDigitalSpecimenRecord(any());

    // When
    rollbackService.rollbackUpdatedSpecimens(specimenRecords, true, true);

    // Then
    then(elasticSearchRepository).should().rollbackVersion(originalSpecimen);
    then(specimenRepository).should().createDigitalSpecimenRecord(List.of(originalSpecimen));
    then(handleComponent).should().rollbackHandleUpdate(anyList());
    then(handleComponent).should(times(1)).rollbackHandleCreation(List.of(MEDIA_PID));
    then(rabbitMqService).should(times(1)).deadLetterEventSpecimen(any());
  }

  // Elastic Insert Failures

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
    var result = rollbackService.handlePartiallyFailedElasticInsertSpecimen(specimenRecords, Map.of(),
        bulkResponse);

    // Then
    then(handleComponent).should().rollbackHandleCreation(List.of(SECOND_HANDLE));
    then(rabbitMqService).should(times(2)).publishCreateEventSpecimen(any());
    then(rabbitMqService).should(times(1)).deadLetterEventSpecimen(any());
    then(specimenRepository).should().rollbackSpecimen(SECOND_HANDLE);
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
    doThrow(JsonProcessingException.class).when(rabbitMqService).publishCreateEventSpecimen(firstRecord);

    // When
    var result = rollbackService.handlePartiallyFailedElasticInsertSpecimen(specimenRecords, Map.of(),
        bulkResponse);

    // Then
    then(handleComponent).should().rollbackHandleCreation(List.of(HANDLE, SECOND_HANDLE));
    then(rabbitMqService).should(times(2)).publishCreateEventSpecimen(any());
    then(rabbitMqService).should(times(2)).deadLetterEventSpecimen(any());
    then(specimenRepository).should().rollbackSpecimen(SECOND_HANDLE);
    then(specimenRepository).should().rollbackSpecimen(HANDLE);
    then(specimenRepository).shouldHaveNoMoreInteractions();
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
    var result = rollbackService.handlePartiallyFailedElasticInsertSpecimen(specimenRecords,
        Map.of(new DigitalMediaKey(SECOND_HANDLE, MEDIA_URL), MEDIA_PID), bulkResponse);

    // Then
    then(handleComponent).should().rollbackHandleCreation(List.of(SECOND_HANDLE, MEDIA_PID));
    then(rabbitMqService).should(times(2)).publishCreateEventSpecimen(any());
    then(rabbitMqService).should(times(1)).deadLetterEventSpecimen(any());
    then(specimenRepository).should().rollbackSpecimen(SECOND_HANDLE);
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
    var result = rollbackService.handlePartiallyFailedElasticInsertSpecimen(specimenRecords,
        givenMediaPidResponse(), bulkResponse);

    // Then
    then(handleComponent).should().rollbackHandleCreation(List.of(SECOND_HANDLE));
    then(rabbitMqService).should(times(2)).publishCreateEventSpecimen(any());
    then(rabbitMqService).should(times(1)).deadLetterEventSpecimen(any());
    then(rabbitMqService).should(times(1)).publishAnnotationRequestEventSpecimen(any(), any());
    then(specimenRepository).should().rollbackSpecimen(SECOND_HANDLE);
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
    doThrow(JsonProcessingException.class).when(rabbitMqService)
        .publishAnnotationRequestEventSpecimen(any(), eq(givenDigitalSpecimenRecord()));

    // When
    var result = rollbackService.handlePartiallyFailedElasticInsertSpecimen(specimenRecords,
        givenMediaPidResponse(), bulkResponse);

    // Then
    then(handleComponent).should().rollbackHandleCreation(List.of(SECOND_HANDLE));
    then(rabbitMqService).should(times(2)).publishCreateEventSpecimen(any());
    then(rabbitMqService).should(times(1)).deadLetterEventSpecimen(any());
    then(rabbitMqService).should(times(1)).publishAnnotationRequestEventSpecimen(any(), any());
    then(specimenRepository).should().rollbackSpecimen(SECOND_HANDLE);
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

  // Elastic Update Failure
  @Test
  void testHandlePartiallyFailedElasticUpdateSpecimenNoMedia() throws Exception {
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
    given(fdoRecordService.handleNeedsUpdateSpecimen(any(), any())).willReturn(true);

    // When
    var result = rollbackService.handlePartiallyFailedElasticUpdateSpecimen(specimenRecords,
        bulkResponse);

    // Then
    assertThat(result).hasSize(2);
    then(rabbitMqService).should(times(2)).publishUpdateEventSpecimen(any(), any());
    then(rabbitMqService).should(times(1)).deadLetterEventSpecimen(any());
    then(rabbitMqService).shouldHaveNoMoreInteractions();
    then(specimenRepository).should()
        .createDigitalSpecimenRecord(List.of(secondSpecimenUpdated.currentDigitalSpecimen()));
    then(elasticSearchRepository).shouldHaveNoMoreInteractions();
    then(handleComponent).should().rollbackHandleUpdate(anyList());
    then(handleComponent).shouldHaveNoMoreInteractions();
  }

  @Test
  void testHandlePartiallyFailedElasticUpdateSpecimenFailureHasMedia() throws Exception {
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
    given(fdoRecordService.handleNeedsUpdateSpecimen(any(), any())).willReturn(true);

    // When
    var result = rollbackService.handlePartiallyFailedElasticUpdateSpecimen(specimenRecords,
        bulkResponse);

    // Then
    assertThat(result).hasSize(2);
    then(rabbitMqService).should(times(2)).publishUpdateEventSpecimen(any(), any());
    then(rabbitMqService).should(times(1)).deadLetterEventSpecimen(any());
    then(rabbitMqService).shouldHaveNoMoreInteractions();
    then(specimenRepository).should()
        .createDigitalSpecimenRecord(List.of(secondSpecimenUpdated.currentDigitalSpecimen()));
    then(elasticSearchRepository).shouldHaveNoMoreInteractions();
    then(handleComponent).should().rollbackHandleUpdate(anyList());
    then(handleComponent).should().rollbackHandleCreation(List.of(MEDIA_PID));
  }

  @Test
  void testHandlePartiallyFailedElasticUpdateSpecimenSuccessHasMedia() throws Exception {
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
    given(fdoRecordService.handleNeedsUpdateSpecimen(any(), any())).willReturn(true);

    // When
    var result = rollbackService.handlePartiallyFailedElasticUpdateSpecimen(specimenRecords,
        bulkResponse);

    // Then
    assertThat(result).hasSize(2);
    then(rabbitMqService).should(times(2)).publishUpdateEventSpecimen(any(), any());
    then(rabbitMqService).should(times(1)).deadLetterEventSpecimen(any());
    then(rabbitMqService).shouldHaveNoMoreInteractions();
    then(specimenRepository).should()
        .createDigitalSpecimenRecord(List.of(secondSpecimenUpdated.currentDigitalSpecimen()));
    then(elasticSearchRepository).shouldHaveNoMoreInteractions();
    then(handleComponent).should().rollbackHandleUpdate(anyList());
    then(handleComponent).shouldHaveNoMoreInteractions();
  }

  @Test
  void testHandlePartiallyFailedElasticUpdateSuccessHasMediaKafkaPublishingFailedSpecimen()
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
    given(fdoRecordService.handleNeedsUpdateSpecimen(any(), any())).willReturn(true);
    doThrow(JsonProcessingException.class).when(rabbitMqService)
        .publishUpdateEventSpecimen(eq(givenUnequalDigitalSpecimenRecord()), any());

    // When
    var result = rollbackService.handlePartiallyFailedElasticUpdateSpecimen(specimenRecords,
        bulkResponse);

    // Then
    assertThat(result).hasSize(1);
    then(rabbitMqService).should(times(2)).publishUpdateEventSpecimen(any(), any());
    then(rabbitMqService).should(times(2)).deadLetterEventSpecimen(any());
    then(rabbitMqService).shouldHaveNoMoreInteractions();
    then(specimenRepository).should(times(2)).createDigitalSpecimenRecord(anyList());
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
    then(rabbitMqService).should().deadLetterEventSpecimen(givenDigitalSpecimenEvent());
  }

  @Test
  void testPidCreationFailedRollbackFailedDlqFailed() throws Exception {
    // Given
    doThrow(PidException.class).when(handleComponent)
        .rollbackFromPhysId(List.of(PHYSICAL_SPECIMEN_ID));
    doThrow(JsonProcessingException.class).when(rabbitMqService)
        .deadLetterEventSpecimen(givenDigitalSpecimenEvent());

    // When / Then
    assertDoesNotThrow(
        () -> rollbackService.pidCreationFailed(List.of(givenDigitalSpecimenEvent())));
  } */


}
