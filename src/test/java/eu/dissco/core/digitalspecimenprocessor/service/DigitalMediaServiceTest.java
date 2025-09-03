package eu.dissco.core.digitalspecimenprocessor.service;

import static eu.dissco.core.digitalspecimenprocessor.domain.AgentRoleType.PROCESSING_SERVICE;
import static eu.dissco.core.digitalspecimenprocessor.schema.Agent.Type.SCHEMA_SOFTWARE_APPLICATION;
import static eu.dissco.core.digitalspecimenprocessor.schema.Identifier.DctermsType.DOI;
import static eu.dissco.core.digitalspecimenprocessor.util.AgentUtils.createMachineAgent;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.ANOTHER_SPECIMEN_NAME;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.APP_HANDLE;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.APP_NAME;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.CREATED;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.DOI_PREFIX;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.HANDLE;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MAPPER;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MEDIA_MAS;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MEDIA_PID;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MEDIA_PID_ALT;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MEDIA_URL;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MEDIA_URL_ALT;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.ORGANISATION_ID;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.VERSION;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalMedia;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalMediaEvent;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalMediaEventWithSpecimenEr;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalMediaRecord;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenEvent;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenJsonPatchMedia;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenPidProcessResultMedia;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenUnequalDigitalMediaEvent;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenUnequalDigitalMediaRecord;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenUnequalDigitalSpecimenRecord;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenUpdatedDigitalMediaTuple;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mockStatic;

import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dissco.core.digitalspecimenprocessor.domain.FdoType;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaEvent;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaWrapper;
import eu.dissco.core.digitalspecimenprocessor.domain.media.UpdatedDigitalMediaRecord;
import eu.dissco.core.digitalspecimenprocessor.domain.media.UpdatedDigitalMediaTuple;
import eu.dissco.core.digitalspecimenprocessor.domain.relation.MediaRelationshipProcessResult;
import eu.dissco.core.digitalspecimenprocessor.domain.relation.PidProcessResult;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.UpdatedDigitalSpecimenTuple;
import eu.dissco.core.digitalspecimenprocessor.exception.PidException;
import eu.dissco.core.digitalspecimenprocessor.repository.DigitalMediaRepository;
import eu.dissco.core.digitalspecimenprocessor.repository.ElasticSearchRepository;
import eu.dissco.core.digitalspecimenprocessor.schema.EntityRelationship;
import eu.dissco.core.digitalspecimenprocessor.web.HandleComponent;
import java.net.URI;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.jooq.exception.DataAccessException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DigitalMediaServiceTest {

  private static MockedStatic<Instant> mockedInstant;
  private static MockedStatic<Clock> mockedClock;
  @Mock
  private DigitalMediaRepository repository;
  @Mock
  private FdoRecordService fdoRecordService;
  @Mock
  private HandleComponent handleComponent;
  @Mock
  private ObjectMapper mapper;
  @Mock
  private RollbackService rollbackService;
  @Mock
  private ElasticSearchRepository elasticRepository;
  @Mock
  private AnnotationPublisherService annotationPublisherService;
  @Mock
  private RabbitMqPublisherService publisherService;
  @Mock
  private BulkResponse bulkResponse;
  private DigitalMediaService mediaService;

  @BeforeAll
  static void init() {
    Clock clock = Clock.fixed(CREATED, ZoneOffset.UTC);
    Instant instant = Instant.now(clock);
    mockedInstant = mockStatic(Instant.class);
    mockedInstant.when(Instant::now).thenReturn(instant);
    mockedInstant.when(() -> Instant.from(any())).thenReturn(instant);
    mockedInstant.when(() -> Instant.parse(any())).thenReturn(instant);
    mockedClock = mockStatic(Clock.class);
    mockedClock.when(Clock::systemUTC).thenReturn(clock);
  }

  @AfterAll
  static void teardown() {
    mockedInstant.close();
    mockedClock.close();
  }

  @BeforeEach
  void setup() {
    mediaService = new DigitalMediaService(repository, fdoRecordService, handleComponent, MAPPER,
        rollbackService, elasticRepository, annotationPublisherService, publisherService);
  }

  @Test
  void testUpdateEqualMedia() {
    // Given

    // When
    mediaService.updateEqualDigitalMedia(List.of(givenDigitalMediaRecord()));

    // Then
    then(repository).should().updateLastChecked(List.of(MEDIA_PID));
  }

  @Test
  void testCreateNewMedia() throws Exception {
    // Given
    var pidMap = Map.of(MEDIA_URL, givenPidProcessResultMedia());
    var events = List.of(givenDigitalMediaEvent());
    var records = Set.of(givenDigitalMediaRecord());
    given(bulkResponse.errors()).willReturn(false);
    given(elasticRepository.indexDigitalMedia(records)).willReturn(bulkResponse);

    // When
    var result = mediaService.createNewDigitalMedia(events, pidMap);

    // Then
    assertThat(result).isEqualTo(records);
    then(repository).should().createDigitalMediaRecord(records);
    then(annotationPublisherService).should().publishAnnotationNewMedia(records);
    then(publisherService).should()
        .publishAnnotationRequestEventMedia(MEDIA_MAS, givenDigitalMediaRecord());
  }

  @Test
  void testCreateNewMediaMasFailed() throws Exception {
    // Given
    var pidMap = Map.of(MEDIA_URL, givenPidProcessResultMedia());
    var events = List.of(givenDigitalMediaEvent());
    var records = Set.of(givenDigitalMediaRecord());
    given(bulkResponse.errors()).willReturn(false);
    given(elasticRepository.indexDigitalMedia(records)).willReturn(bulkResponse);
    doThrow(JsonProcessingException.class).when(publisherService)
        .publishAnnotationRequestEventMedia(any(), any());

    // When
    var result = mediaService.createNewDigitalMedia(events, pidMap);

    // Then
    assertThat(result).isEqualTo(records);
    then(repository).should().createDigitalMediaRecord(records);
    then(annotationPublisherService).should().publishAnnotationNewMedia(records);
    then(publisherService).should()
        .publishAnnotationRequestEventMedia(MEDIA_MAS, givenDigitalMediaRecord());
  }

  @Test
  void testCreateNewMediaNoPids() {
    // Given
    var pidMap = Map.of(MEDIA_URL_ALT, givenPidProcessResultMedia());
    var events = List.of(givenDigitalMediaEvent());

    // When
    var result = mediaService.createNewDigitalMedia(events, pidMap);

    // Then
    assertThat(result).isEmpty();
  }

  @Test
  void testCreateNewMediaDatabaseException() {
    // Given
    var pidMap = Map.of(MEDIA_URL, givenPidProcessResultMedia());
    var events = List.of(givenDigitalMediaEvent());
    var records = Set.of(givenDigitalMediaRecord());
    doThrow(DataAccessException.class).when(repository).createDigitalMediaRecord(records);

    // When
    var result = mediaService.createNewDigitalMedia(events, pidMap);

    // Then
    assertThat(result).isEmpty();
    then(rollbackService).should().rollbackNewMedias(events, pidMap, false, false);
  }

  @Test
  void testCreateNewMediaElasticFailure() throws Exception {
    // Given
    var pidMap = Map.of(MEDIA_URL, givenPidProcessResultMedia());
    var events = List.of(givenDigitalMediaEvent());
    var records = Set.of(givenDigitalMediaRecord());
    doThrow(ElasticsearchException.class).when(elasticRepository).indexDigitalMedia(records);

    // When
    var result = mediaService.createNewDigitalMedia(events, pidMap);

    // Then
    assertThat(result).isEmpty();
    then(repository).should().createDigitalMediaRecord(records);
    then(rollbackService).should().rollbackNewMedias(events, pidMap, false, true);
  }


  @Test
  void testCreateNewMediaElasticPartialFailure() throws Exception {
    // Given
    var pidMap = Map.of(MEDIA_URL, givenPidProcessResultMedia(), MEDIA_URL_ALT,
        new PidProcessResult(MEDIA_PID_ALT, Set.of(HANDLE)));
    var events = List.of(givenDigitalMediaEvent(), givenUnequalDigitalMediaEvent());
    var successfulRecord = givenDigitalMediaRecord();
    var records = Set.of(successfulRecord,
        givenUnequalDigitalMediaRecord(MEDIA_PID_ALT, MEDIA_URL_ALT, VERSION));
    var successfulRecordMap = Map.of(successfulRecord, Set.of(MEDIA_MAS));
    given(bulkResponse.errors()).willReturn(true);
    given(elasticRepository.indexDigitalMedia(records)).willReturn(bulkResponse);
    given(rollbackService.handlePartiallyFailedElasticInsertMedia(anyMap(), any(), any()))
        .willReturn(successfulRecordMap);

    // When
    var result = mediaService.createNewDigitalMedia(events, pidMap);

    // Then
    assertThat(result).isEqualTo(Set.of(successfulRecord));
    then(repository).should().createDigitalMediaRecord(records);
    then(publisherService).shouldHaveNoMoreInteractions();
  }

  @Test
  void testCreateNewMediaPublishingFails() throws Exception {
    // Given
    var pidMap = Map.of(MEDIA_URL, givenPidProcessResultMedia(), MEDIA_URL_ALT,
        new PidProcessResult(MEDIA_PID_ALT, Set.of(HANDLE)));
    var events = List.of(givenDigitalMediaEvent(), givenUnequalDigitalMediaEvent(MEDIA_URL_ALT));
    var records = Set.of(givenDigitalMediaRecord(),
        givenUnequalDigitalMediaRecord(MEDIA_PID_ALT, MEDIA_URL_ALT, VERSION));
    given(bulkResponse.errors()).willReturn(false);
    given(elasticRepository.indexDigitalMedia(records)).willReturn(bulkResponse);
    lenient().doThrow(JsonProcessingException.class).when(publisherService)
        .publishCreateEventMedia(givenDigitalMediaRecord());

    // When
    var result = mediaService.createNewDigitalMedia(events, pidMap);

    // Then
    assertThat(result).isEqualTo(
        Set.of(givenUnequalDigitalMediaRecord(MEDIA_PID_ALT, MEDIA_URL_ALT, VERSION)));
    then(rollbackService).should()
        .rollbackNewMedias(List.of(givenDigitalMediaEventWithSpecimenEr()), pidMap, true, true);
  }

  @Test
  void testUpdateExistingMedia() throws Exception {
    // Given
    var pidMap = Map.of(MEDIA_URL, givenPidProcessResultMedia());
    var tuples = List.of(givenUpdatedDigitalMediaTuple(false));
    var records = Set.of(givenDigitalMediaRecord(VERSION + 1));
    given(bulkResponse.errors()).willReturn(false);
    given(elasticRepository.indexDigitalMedia(records)).willReturn(bulkResponse);

    // When
    var result = mediaService.updateExistingDigitalMedia(tuples, pidMap);

    // Then
    assertThat(result).isEqualTo(records);
    then(handleComponent).shouldHaveNoInteractions();
    then(annotationPublisherService).should().publishAnnotationUpdatedMedia(any());
    then(publisherService).should()
        .publishUpdateEventMedia(eq(givenDigitalMediaRecord(VERSION + 1)), any());
  }

  @Test
  void testUpdateExistingMediaUpdateHandle() throws Exception {
    // Given
    var pidMap = Map.of(MEDIA_URL, givenPidProcessResultMedia());
    var tuples = List.of(givenUpdatedDigitalMediaTuple(false));
    var records = Set.of(givenDigitalMediaRecord(VERSION + 1));
    given(fdoRecordService.handleNeedsUpdateMedia(any(), any())).willReturn(true);
    given(bulkResponse.errors()).willReturn(false);
    given(elasticRepository.indexDigitalMedia(records)).willReturn(bulkResponse);

    // When
    var result = mediaService.updateExistingDigitalMedia(tuples, pidMap);

    // Then
    assertThat(result).isEqualTo(records);
    then(handleComponent).should().updateHandle(any());
    then(annotationPublisherService).should().publishAnnotationUpdatedMedia(any());
    then(publisherService).should()
        .publishUpdateEventMedia(eq(givenDigitalMediaRecord(VERSION + 1)), any());
  }

  @Test
  void testUpdateExistingMediaUpdateHandleFailed() throws Exception {
    // Given
    var pidMap = Map.of(MEDIA_URL, givenPidProcessResultMedia());
    var tuples = List.of(givenUpdatedDigitalMediaTuple(false));
    given(fdoRecordService.handleNeedsUpdateMedia(any(), any())).willReturn(true);
    doThrow(PidException.class).when(handleComponent).updateHandle(any());

    // When
    var result = mediaService.updateExistingDigitalMedia(tuples, pidMap);

    // Then
    assertThat(result).isEmpty();
    then(handleComponent).should().updateHandle(any());
    then(publisherService).should().deadLetterEventMedia(any());
    then(annotationPublisherService).shouldHaveNoInteractions();
  }

  @Test
  void testUpdateExistingMediaUpdateHandleFailedDlqFailed() throws Exception {
    // Given
    var pidMap = Map.of(MEDIA_URL, givenPidProcessResultMedia());
    var tuples = List.of(givenUpdatedDigitalMediaTuple(false));
    given(fdoRecordService.handleNeedsUpdateMedia(any(), any())).willReturn(true);
    doThrow(PidException.class).when(handleComponent).updateHandle(any());
    doThrow(JsonProcessingException.class).when(publisherService).deadLetterEventMedia(any());

    // When
    var result = mediaService.updateExistingDigitalMedia(tuples, pidMap);

    // Then
    assertThat(result).isEmpty();
    then(handleComponent).should().updateHandle(any());
  }

  @Test
  void testUpdateExistingMediaElasticFailed() throws Exception {
    // Given
    var pidMap = Map.of(MEDIA_URL, givenPidProcessResultMedia());
    var tuples = List.of(givenUpdatedDigitalMediaTuple(false));
    var records = Set.of(givenDigitalMediaRecord(VERSION + 1));
    var updatedRecord = Set.of(new UpdatedDigitalMediaRecord(
        givenDigitalMediaRecord(VERSION + 1),
        Set.of(MEDIA_MAS),
        givenUnequalDigitalMediaRecord(),
        givenJsonPatchMedia()
    ));
    doThrow(ElasticsearchException.class).when(elasticRepository).indexDigitalMedia(records);

    // When
    var result = mediaService.updateExistingDigitalMedia(tuples, pidMap);

    // Then
    assertThat(result).isEmpty();
    then(rollbackService).should().rollbackUpdatedMedias(updatedRecord, false, true, List.of(
        givenDigitalMediaEventWithSpecimenEr()), pidMap);
    then(annotationPublisherService).shouldHaveNoInteractions();
  }

  @Test
  void testUpdateExistingMediaElasticPartiallyFailed() throws Exception {
    // Given
    var pidMap = Map.of(MEDIA_URL, givenPidProcessResultMedia(), MEDIA_URL_ALT,
        new PidProcessResult(MEDIA_PID_ALT, Set.of(HANDLE)));
    var successfulRecord = givenDigitalMediaRecord(2);
    var records = Set.of(
        successfulRecord,
        givenUnequalDigitalMediaRecord(MEDIA_PID_ALT, MEDIA_URL_ALT, VERSION + 1));
    given(bulkResponse.errors()).willReturn(true);
    given(elasticRepository.indexDigitalMedia(records)).willReturn(bulkResponse);
    var tuples = List.of(
        givenUpdatedDigitalMediaTuple(false),
        new UpdatedDigitalMediaTuple(
            givenUnequalDigitalMediaRecord(MEDIA_PID_ALT, MEDIA_URL_ALT, 1),
            givenUnequalDigitalMediaEvent(),
            Set.of()
        )
    );
    given(rollbackService.handlePartiallyFailedElasticUpdateMedia(any(), eq(bulkResponse), any(),
        eq(pidMap)))
        .willReturn(Set.of(
            new UpdatedDigitalMediaRecord(
                successfulRecord,
                Set.of(MEDIA_MAS),
                givenUnequalDigitalMediaRecord(),
                givenJsonPatchMedia()
            )
        ));

    // When
    var result = mediaService.updateExistingDigitalMedia(tuples, pidMap);

    // Then
    assertThat(result).isEqualTo(Set.of(successfulRecord));
    then(handleComponent).shouldHaveNoInteractions();
    then(publisherService).should()
        .publishUpdateEventMedia(givenDigitalMediaRecord(VERSION + 1), givenJsonPatchMedia());
  }

  @Test
  void testUpdateExistingMediaPublishingFailed() throws Exception {
    // Given
    var pidMap = Map.of(MEDIA_URL, givenPidProcessResultMedia());
    var tuples = List.of(givenUpdatedDigitalMediaTuple(false));
    var records = Set.of(givenDigitalMediaRecord(VERSION + 1));
    given(bulkResponse.errors()).willReturn(false);
    given(elasticRepository.indexDigitalMedia(records)).willReturn(bulkResponse);
    doThrow(JsonProcessingException.class).when(publisherService)
        .publishUpdateEventMedia(any(), any());

    // When
    var result = mediaService.updateExistingDigitalMedia(tuples, pidMap);

    // Then
    assertThat(result).isEmpty();
    then(rollbackService).should().rollbackUpdatedMedias(any(), eq(true), eq(true), eq(List.of(
        givenDigitalMediaEventWithSpecimenEr())), eq(pidMap));
  }

  @Test
  void testUpdateExistingMediaDatabaseFailed() {
    // Given
    var pidMap = Map.of(MEDIA_URL, givenPidProcessResultMedia());
    var tuples = List.of(givenUpdatedDigitalMediaTuple(false));
    var records = Set.of(givenDigitalMediaRecord(VERSION + 1));
    doThrow(DataAccessException.class).when(repository).createDigitalMediaRecord(records);

    // When
    var result = mediaService.updateExistingDigitalMedia(tuples, pidMap);

    // Then
    assertThat(result).isEmpty();
  }

  @Test
  void testTombstoneSpecimenRelations() throws JsonProcessingException {
    // Given
    var tuple = new UpdatedDigitalSpecimenTuple(
        givenUnequalDigitalSpecimenRecord(HANDLE, ANOTHER_SPECIMEN_NAME, ORGANISATION_ID, false),
        givenDigitalSpecimenEvent(true),
        new MediaRelationshipProcessResult(
            List.of(new EntityRelationship()
                .withType("ods:EntityRelationship")
                .withDwcRelationshipOfResource("hasDigitalMedia")
                .withDwcRelationshipEstablishedDate(Date.from(CREATED))
                .withDwcRelatedResourceID(DOI_PREFIX + HANDLE)
                .withOdsRelatedResourceURI(URI.create(DOI_PREFIX + HANDLE))
                .withOdsHasAgents(List.of(createMachineAgent(APP_NAME, APP_HANDLE,
                    PROCESSING_SERVICE, DOI, SCHEMA_SOFTWARE_APPLICATION)))),
            List.of(),
            List.of()));
    given(repository.getExistingDigitalMediaByDoi(Set.of(HANDLE))).willReturn(
        List.of(givenDigitalMedia(
            MEDIA_URL)));
    var expected = new DigitalMediaEvent(
        Collections.emptySet(), new DigitalMediaWrapper(
        FdoType.MEDIA.getPid(),
        givenDigitalMedia(MEDIA_URL, Collections.emptyList()),
        MAPPER.createObjectNode()
    ), false
    );

    // When
    mediaService.tombstoneSpecimenRelations(List.of(tuple));

    // Then
    then(publisherService).should().republishMediaEvent(expected);
  }

}
