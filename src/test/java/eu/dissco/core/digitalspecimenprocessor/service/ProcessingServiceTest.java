package eu.dissco.core.digitalspecimenprocessor.service;

import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.ANOTHER_ORGANISATION;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.ANOTHER_SPECIMEN_NAME;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.APP_HANDLE;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.APP_NAME;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.CREATED;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.HANDLE;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MAPPER;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MAS;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MEDIA_PID;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MEDIA_PID_ALT;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MEDIA_URL;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MIDS_LEVEL;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.ORGANISATION_ID;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.PHYSICAL_SPECIMEN_ID;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.SECOND_HANDLE;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.THIRD_HANDLE;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.VERSION;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDifferentUnequalSpecimen;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalMediaEvent;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalMediaEventWithRelationship;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenEvent;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenRecord;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenRecordWithMediaEr;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenWithEntityRelationship;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenWrapper;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenEmptyMediaProcessResult;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenEmptyMediaProcessResultMap;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenHandleComponentResponse;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenHandleRequest;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenJsonPatch;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenMediaPidResponse;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenMediaProcessResultMapNew;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenMediaProcessResultUnchanged;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenUnequalDigitalSpecimenRecord;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenUpdatedDigitalSpecimenRecord;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import co.elastic.clients.elasticsearch.core.BulkResponse;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.nimbusds.jose.util.Pair;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaEvent;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaKey;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaProcessResult;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenEvent;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenRecord;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenWrapper;
import eu.dissco.core.digitalspecimenprocessor.exception.DisscoRepositoryException;
import eu.dissco.core.digitalspecimenprocessor.exception.PidException;
import eu.dissco.core.digitalspecimenprocessor.property.ApplicationProperties;
import eu.dissco.core.digitalspecimenprocessor.repository.DigitalSpecimenRepository;
import eu.dissco.core.digitalspecimenprocessor.repository.ElasticSearchRepository;
import eu.dissco.core.digitalspecimenprocessor.schema.DigitalSpecimen;
import eu.dissco.core.digitalspecimenprocessor.schema.DigitalSpecimen.OdsTopicDiscipline;
import eu.dissco.core.digitalspecimenprocessor.utils.TestUtils;
import eu.dissco.core.digitalspecimenprocessor.web.HandleComponent;
import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.jooq.exception.DataAccessException;
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
  private AnnotationPublisherService annotationPublisherService;
  @Mock
  private HandleComponent handleComponent;
  @Mock
  private ApplicationProperties applicationProperties;
  @Mock
  private DigitalMediaService digitalMediaService;
  @Mock
  private RollbackService rollbackService;
  @Mock
  private EqualityService equalityService;

  private MockedStatic<Instant> mockedInstant;
  private MockedStatic<Clock> mockedClock;
  private ProcessingService service;

  @BeforeEach
  void setup() {
    service = new ProcessingService(repository, fdoRecordService, elasticRepository, kafkaService,
        midsService, handleComponent, applicationProperties, annotationPublisherService, MAPPER,
        digitalMediaService, equalityService, rollbackService);
    Clock clock = Clock.fixed(CREATED, ZoneOffset.UTC);
    Instant instant = Instant.now(clock);
    mockedInstant = mockStatic(Instant.class);
    mockedInstant.when(Instant::now).thenReturn(instant);
    mockedInstant.when(() -> Instant.from(any())).thenReturn(instant);
    mockedInstant.when(() -> Instant.parse(any())).thenReturn(instant);
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
        List.of(givenDigitalSpecimenWithEntityRelationship()));
    given(equalityService.isEqual(any(), any(), any())).willReturn(true);
    given(applicationProperties.getPid()).willReturn(APP_HANDLE);
    given(applicationProperties.getName()).willReturn(APP_NAME);
    given(digitalMediaService.getExistingDigitalMedia(any(), anyList())).willReturn(
        Map.of(HANDLE,
            givenMediaProcessResultUnchanged(List.of(givenDigitalSpecimenEvent(true, true)))));

    // When
    List<DigitalSpecimenRecord> result = service.handleMessages(
        List.of(givenDigitalSpecimenEvent(true, true)));

    // Then
    verifyNoInteractions(handleComponent);
    verifyNoInteractions(fdoRecordService);
    verifyNoInteractions(annotationPublisherService);
    then(repository).should().updateLastChecked(List.of(HANDLE));
    then(kafkaService).should(times(1))
        .publishDigitalMediaObject(givenDigitalMediaEventWithRelationship());
    assertThat(result).isEmpty();
  }

  @Test
  void testEqualSpecimenNoMedia() throws DisscoRepositoryException {
    // Given
    given(repository.getDigitalSpecimens(List.of(PHYSICAL_SPECIMEN_ID))).willReturn(
        List.of(givenDigitalSpecimenRecord()));
    given(digitalMediaService.getExistingDigitalMedia(any(), anyList())).willReturn(Map.of(
        HANDLE, givenEmptyMediaProcessResult()));
    given(equalityService.isEqual(any(), any(), eq(givenEmptyMediaProcessResult()))).willReturn(
        true);

    // When
    List<DigitalSpecimenRecord> result = service.handleMessages(
        List.of(givenDigitalSpecimenEvent(false, false)));

    // Then
    verifyNoInteractions(handleComponent);
    verifyNoInteractions(fdoRecordService);
    verifyNoInteractions(annotationPublisherService);
    then(repository).should().updateLastChecked(List.of(HANDLE));
    then(kafkaService).shouldHaveNoInteractions();
    assertThat(result).isEmpty();
  }

  @Test
  void testUnequalSpecimen() throws Exception {
    // Given
    var currentSpecimenRecord = new DigitalSpecimenRecord(HANDLE, MIDS_LEVEL, VERSION, CREATED,
        new DigitalSpecimenWrapper(PHYSICAL_SPECIMEN_ID, ANOTHER_SPECIMEN_NAME,
            new DigitalSpecimen().withOdsTopicDiscipline(OdsTopicDiscipline.ECOLOGY), null));
    var expected = List.of(givenDigitalSpecimenRecord(2, true));
    given(equalityService.isEqual(any(), any(), any())).willReturn(false);
    mockEqualityServiceSetDates(List.of(givenDigitalSpecimenEvent(true)));
    given(repository.getDigitalSpecimens(List.of(PHYSICAL_SPECIMEN_ID))).willReturn(
        List.of(currentSpecimenRecord));
    given(bulkResponse.errors()).willReturn(false);
    given(elasticRepository.indexDigitalSpecimen(expected)).willReturn(bulkResponse);
    given(midsService.calculateMids(givenDigitalSpecimenWrapper(true))).willReturn(1);
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);
    given(applicationProperties.getPid()).willReturn(APP_HANDLE);
    given(applicationProperties.getName()).willReturn(APP_NAME);
    given(digitalMediaService.getExistingDigitalMedia(any(), anyList())).willReturn(
        Map.of(HANDLE, givenEmptyMediaProcessResult()));

    // When
    var result = service.handleMessages(List.of(givenDigitalSpecimenEvent(true)));

    // Then
    then(fdoRecordService).should().buildUpdateHandleRequest(anyList());
    then(handleComponent).should().updateHandle(any());
    then(repository).should().createDigitalSpecimenRecord(expected);
    then(kafkaService).should()
        .publishUpdateEvent(eq(givenDigitalSpecimenRecord(2, true)), any(JsonNode.class));
    then(kafkaService).should()
        .publishDigitalMediaObject(givenDigitalMediaEventWithRelationship());
    assertThat(result).isEqualTo(List.of(givenDigitalSpecimenRecord(2, true)));
    then(annotationPublisherService).should().publishAnnotationUpdatedSpecimen(anySet());
  }

  @Test
  void testUnequalSpecimenNewMedia() throws Exception {
    // Given
    var expected = List.of(
        givenDigitalSpecimenRecordWithMediaEr(HANDLE, PHYSICAL_SPECIMEN_ID, false, 2));
    var currentSpecimenRecord = givenDigitalSpecimenRecord();
    var currentMediaEvent = List.of(givenDigitalMediaEvent());
    var newMediaResult = new DigitalMediaProcessResult(List.of(), List.of(), currentMediaEvent);
    given(equalityService.isEqual(any(), any(), eq(newMediaResult))).willReturn(false);
    mockEqualityServiceSetDates(List.of(givenDigitalSpecimenEvent(true)));
    given(repository.getDigitalSpecimens(List.of(PHYSICAL_SPECIMEN_ID))).willReturn(
        List.of(currentSpecimenRecord));
    given(bulkResponse.errors()).willReturn(false);
    given(elasticRepository.indexDigitalSpecimen(expected)).willReturn(bulkResponse);
    given(midsService.calculateMids(givenDigitalSpecimenWrapper(true))).willReturn(1);
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);
    given(applicationProperties.getPid()).willReturn(APP_HANDLE);
    given(applicationProperties.getName()).willReturn(APP_NAME);
    given(digitalMediaService.getExistingDigitalMedia(any(), anyList())).willReturn(
        Map.of(HANDLE, newMediaResult));
    given(handleComponent.postMediaHandle(any())).willReturn(givenMediaPidResponse());

    // When
    var result = service.handleMessages(List.of(givenDigitalSpecimenEvent(true)));

    // Then
    then(fdoRecordService).should().buildUpdateHandleRequest(anyList());
    then(handleComponent).should().updateHandle(any());
    then(repository).should().createDigitalSpecimenRecord(expected);
    then(kafkaService).should()
        .publishUpdateEvent(eq(expected.get(0)), any(JsonNode.class));
    then(kafkaService).should()
        .publishDigitalMediaObject(givenDigitalMediaEventWithRelationship());
    assertThat(result).isEqualTo(expected);
    then(annotationPublisherService).should().publishAnnotationUpdatedSpecimen(anySet());
  }

  @Test
  void testUnequalSpecimenTombstoneMedia() throws Exception {
    // Given
    var currentSpecimenRecord = givenDigitalSpecimenRecordWithMediaEr();
    var mediaEr = currentSpecimenRecord.digitalSpecimenWrapper().attributes()
        .getOdsHasEntityRelationships();
    var tombstoneResult = new DigitalMediaProcessResult(List.of(), mediaEr, List.of());
    var expected = List.of(givenDigitalSpecimenRecord(2, true));
    given(equalityService.isEqual(any(), any(), eq(tombstoneResult))).willReturn(false);
    mockEqualityServiceSetDates(List.of(givenDigitalSpecimenEvent(true)));
    given(repository.getDigitalSpecimens(List.of(PHYSICAL_SPECIMEN_ID))).willReturn(
        List.of(currentSpecimenRecord));
    given(bulkResponse.errors()).willReturn(false);
    given(elasticRepository.indexDigitalSpecimen(expected)).willReturn(bulkResponse);
    given(midsService.calculateMids(givenDigitalSpecimenWrapper(true))).willReturn(1);
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);
    given(applicationProperties.getPid()).willReturn(APP_HANDLE);
    given(applicationProperties.getName()).willReturn(APP_NAME);
    given(digitalMediaService.getExistingDigitalMedia(any(), anyList())).willReturn(
        Map.of(HANDLE, tombstoneResult));

    // When
    var result = service.handleMessages(List.of(givenDigitalSpecimenEvent(true)));

    // Then
    then(fdoRecordService).should().buildUpdateHandleRequest(anyList());
    then(handleComponent).should().updateHandle(any());
    then(repository).should().createDigitalSpecimenRecord(expected);
    then(kafkaService).should()
        .publishUpdateEvent(eq(givenDigitalSpecimenRecord(2, true)), any(JsonNode.class));
    then(kafkaService).should()
        .publishDigitalMediaObject(givenDigitalMediaEventWithRelationship());
    assertThat(result).isEqualTo(List.of(givenDigitalSpecimenRecord(2, true)));
    then(digitalMediaService).should().removeSpecimenRelationshipsFromMedia(any());
    then(annotationPublisherService).should().publishAnnotationUpdatedSpecimen(anySet());
  }

  @Test
  void testHandleRecordDoesNotNeedUpdate() throws Exception {
    // Given
    var expected = List.of(givenDigitalSpecimenRecord(2, true));
    given(equalityService.isEqual(any(), any(), any())).willReturn(false);
    mockEqualityServiceSetDates(List.of(givenDigitalSpecimenEvent(true)));
    given(repository.getDigitalSpecimens(List.of(PHYSICAL_SPECIMEN_ID))).willReturn(
        List.of(givenUnequalDigitalSpecimenRecord(HANDLE, ANOTHER_SPECIMEN_NAME, ORGANISATION_ID,
            true)));
    given(bulkResponse.errors()).willReturn(false);
    given(elasticRepository.indexDigitalSpecimen(expected)).willReturn(bulkResponse);
    given(midsService.calculateMids(givenDigitalSpecimenWrapper(true))).willReturn(1);
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(false);
    given(applicationProperties.getPid()).willReturn(APP_HANDLE);
    given(applicationProperties.getName()).willReturn(APP_NAME);
    given(digitalMediaService.getExistingDigitalMedia(any(), anyList())).willReturn(
        Map.of(HANDLE, givenEmptyMediaProcessResult()));

    // When
    var result = service.handleMessages(List.of(givenDigitalSpecimenEvent(true)));

    // Then
    verifyNoMoreInteractions(fdoRecordService);
    verifyNoMoreInteractions(handleComponent);
    then(repository).should().createDigitalSpecimenRecord(expected);
    then(kafkaService).should()
        .publishUpdateEvent(givenDigitalSpecimenRecord(2, true), givenJsonPatch());
    then(kafkaService).should()
        .publishDigitalMediaObject(givenDigitalMediaEventWithRelationship());
    assertThat(result).isEqualTo(List.of(givenDigitalSpecimenRecord(2, true)));
    then(annotationPublisherService).should().publishAnnotationUpdatedSpecimen(anySet());
  }

  @Test
  void testNewSpecimen() throws Exception {
    // Given
    given(repository.getDigitalSpecimens(List.of(PHYSICAL_SPECIMEN_ID))).willReturn(List.of());
    given(bulkResponse.errors()).willReturn(false);
    given(
        elasticRepository.indexDigitalSpecimen(
            Set.of(givenDigitalSpecimenRecordWithMediaEr()))).willReturn(
        bulkResponse);
    given(midsService.calculateMids(givenDigitalSpecimenWrapper(true))).willReturn(1);
    given(
        fdoRecordService.buildPostHandleRequest(
            List.of(givenDigitalSpecimenWrapper(true)))).willReturn(
        List.of(givenHandleRequest()));
    given(handleComponent.postHandle(anyList()))
        .willReturn(givenHandleComponentResponse(List.of(givenDigitalSpecimenRecord())));
    given(handleComponent.postMediaHandle(anyList()))
        .willReturn(givenMediaPidResponse());
    given(fdoRecordService.buildPostRequestMedia(any(), any())).willReturn(
        List.of(MAPPER.createObjectNode()));
    given(applicationProperties.getPid()).willReturn(APP_HANDLE);
    given(applicationProperties.getName()).willReturn(APP_NAME);

    // When
    var result = service.handleMessages(List.of(givenDigitalSpecimenEvent(true)));

    // Then
    then(equalityService).shouldHaveNoInteractions();
    then(repository).should()
        .createDigitalSpecimenRecord(Set.of(givenDigitalSpecimenRecordWithMediaEr()));
    then(kafkaService).should().publishCreateEvent(givenDigitalSpecimenRecordWithMediaEr());
    then(kafkaService).should()
        .publishAnnotationRequestEvent(MAS, givenDigitalSpecimenRecordWithMediaEr());
    then(kafkaService).should()
        .publishDigitalMediaObject(givenDigitalMediaEventWithRelationship(MEDIA_PID));
    assertThat(result).isEqualTo(List.of(givenDigitalSpecimenRecordWithMediaEr()));
    then(annotationPublisherService).should()
        .publishAnnotationNewSpecimen(Set.of(givenDigitalSpecimenRecordWithMediaEr()));
  }

  @Test
  void testNewSpecimenPidFailed() throws Exception {
    // Given
    given(repository.getDigitalSpecimens(List.of(PHYSICAL_SPECIMEN_ID))).willReturn(List.of());
    given(handleComponent.postHandle(anyList())).willThrow(PidException.class);
    given(fdoRecordService.buildPostHandleRequest(anyList())).willReturn(
        List.of(givenHandleRequest()));

    // When
    var result = service.handleMessages(List.of(givenDigitalSpecimenEvent()));

    // Then
    then(equalityService).shouldHaveNoInteractions();
    then(rollbackService).should().pidCreationFailed(List.of(givenDigitalSpecimenEvent()));
    assertThat(result).isEmpty();
  }

  @Test
  void testDuplicateNewSpecimen()
      throws Exception {
    // Given
    var duplicateSpecimen = new DigitalSpecimenEvent(List.of(MAS),
        TestUtils.givenDigitalSpecimenWrapper(PHYSICAL_SPECIMEN_ID, ANOTHER_SPECIMEN_NAME,
            ANOTHER_ORGANISATION, false, false),
        List.of());
    given(repository.getDigitalSpecimens(List.of(PHYSICAL_SPECIMEN_ID))).willReturn(List.of());
    given(bulkResponse.errors()).willReturn(false);
    given(
        elasticRepository.indexDigitalSpecimen(Set.of(givenDigitalSpecimenRecord()))).willReturn(
        bulkResponse);
    given(midsService.calculateMids(givenDigitalSpecimenWrapper())).willReturn(1);
    given(
        fdoRecordService.buildPostHandleRequest(List.of(givenDigitalSpecimenWrapper()))).willReturn(
        List.of(MAPPER.createObjectNode()));
    given(handleComponent.postHandle(anyList()))
        .willReturn(givenHandleComponentResponse(List.of(givenDigitalSpecimenRecord())));

    // When
    var result = service.handleMessages(
        List.of(givenDigitalSpecimenEvent(), duplicateSpecimen));

    // Then
    then(equalityService).shouldHaveNoInteractions();
    verify(handleComponent, times(1)).postHandle(anyList()
    );
    then(repository).should().createDigitalSpecimenRecord(Set.of(givenDigitalSpecimenRecord()));
    then(kafkaService).should().publishCreateEvent(givenDigitalSpecimenRecord());
    then(kafkaService).should().publishAnnotationRequestEvent(MAS, givenDigitalSpecimenRecord());
    then(kafkaService).should().republishEvent(duplicateSpecimen);
    assertThat(result).isEqualTo(List.of(givenDigitalSpecimenRecord()));
  }

  @Test
  void testNewSpecimenIOException()
      throws Exception {
    // Given
    given(repository.getDigitalSpecimens(List.of(PHYSICAL_SPECIMEN_ID))).willReturn(List.of());
    given(
        elasticRepository.indexDigitalSpecimen(Set.of(givenDigitalSpecimenRecord()))).willThrow(
        IOException.class);
    given(fdoRecordService.buildPostHandleRequest(anyList())).willReturn(
        List.of(givenHandleRequest()));
    given(midsService.calculateMids(givenDigitalSpecimenWrapper())).willReturn(1);
    given(handleComponent.postHandle(anyList()))
        .willReturn(givenHandleComponentResponse(List.of(givenDigitalSpecimenRecord())));

    // When
    var result = service.handleMessages(List.of(givenDigitalSpecimenEvent()));

    // Then
    then(equalityService).shouldHaveNoInteractions();
    then(rollbackService).should()
        .rollbackNewSpecimens(anyMap(), eq(Map.of()), eq(false), eq(true));
    then(kafkaService).shouldHaveNoInteractions();
    assertThat(result).isEmpty();
  }

  @Test
  void testNewSpecimenPartialElasticFailed()
      throws Exception {
    // Given
    String secondPhysicalId = "Another Specimen";
    String thirdPhysicalId = "A third Specimen";
    var thirdSpecimen = givenDigitalSpecimenRecordWithMediaEr(THIRD_HANDLE, thirdPhysicalId, false);
    given(repository.getDigitalSpecimens(anyList())).willReturn(List.of());
    given(midsService.calculateMids(any(DigitalSpecimenWrapper.class))).willReturn(1);
    given(fdoRecordService.buildPostHandleRequest(anyList())).willReturn(
        List.of(givenHandleRequest()));
    given(bulkResponse.errors()).willReturn(true);
    given(elasticRepository.indexDigitalSpecimen(anySet())).willReturn(bulkResponse);
    given(applicationProperties.getPid()).willReturn(APP_HANDLE);
    given(applicationProperties.getName()).willReturn(APP_NAME);
    given(fdoRecordService.buildPostRequestMedia(any(), any())).willReturn(List.of(MAPPER.createObjectNode()));
    given(handleComponent.postHandle(anyList()))
        .willReturn(Map.of(
            PHYSICAL_SPECIMEN_ID, HANDLE,
            secondPhysicalId, SECOND_HANDLE,
            thirdPhysicalId, THIRD_HANDLE
        ));
    given(handleComponent.postMediaHandle(any())).willReturn(Map.of(
        new DigitalMediaKey(HANDLE, MEDIA_URL), MEDIA_PID_ALT,
        new DigitalMediaKey(SECOND_HANDLE, MEDIA_URL), MEDIA_PID,
        new DigitalMediaKey(THIRD_HANDLE, MEDIA_URL), MEDIA_PID));
    given(rollbackService.handlePartiallyFailedElasticInsert(any(), any(), eq(bulkResponse)))
        .willReturn(Map.of(
            givenDigitalSpecimenRecord(),
            Pair.of(List.of(MEDIA_PID), List.of(givenDigitalMediaEvent())),
            thirdSpecimen, Pair.of(List.of(), List.of())));

    // When
    var result = service.handleMessages(
        List.of(givenDigitalSpecimenEvent(), givenDigitalSpecimenEvent(secondPhysicalId),
            givenDigitalSpecimenEvent(thirdPhysicalId)));

    // Then
    then(equalityService).shouldHaveNoInteractions();
    then(repository).should().createDigitalSpecimenRecord(anySet());
    then(handleComponent).should().postHandle(any());
    then(kafkaService).should()
        .publishDigitalMediaObject(any(DigitalMediaEvent.class));
    then(kafkaService).shouldHaveNoMoreInteractions();
    assertThat(result).contains(givenDigitalSpecimenRecord(), thirdSpecimen);
    then(annotationPublisherService).should()
        .publishAnnotationNewSpecimen(Set.of(givenDigitalSpecimenRecord(), thirdSpecimen));
  }

  @Test
  void testNewSpecimenKafkaFailed()
      throws Exception {
    // Given
    given(repository.getDigitalSpecimens(List.of(PHYSICAL_SPECIMEN_ID))).willReturn(List.of());
    given(
        fdoRecordService.buildPostHandleRequest(List.of(givenDigitalSpecimenWrapper()))).willReturn(
        List.of(MAPPER.createObjectNode()));
    given(handleComponent.postHandle(anyList()))
        .willReturn(givenHandleComponentResponse(List.of(givenDigitalSpecimenRecord())));
    given(bulkResponse.errors()).willReturn(false);
    given(
        elasticRepository.indexDigitalSpecimen(Set.of(givenDigitalSpecimenRecord()))).willReturn(
        bulkResponse);
    doThrow(JsonProcessingException.class).when(kafkaService)
        .publishCreateEvent(any(DigitalSpecimenRecord.class));
    given(midsService.calculateMids(givenDigitalSpecimenWrapper())).willReturn(1);

    // When
    var result = service.handleMessages(List.of(givenDigitalSpecimenEvent()));

    // Then
    then(equalityService).shouldHaveNoInteractions();
    then(repository).should().createDigitalSpecimenRecord(anySet());
    then(rollbackService).should().rollbackNewSpecimens(any(), anyMap(), eq(true), eq(true));
    then(kafkaService).shouldHaveNoMoreInteractions();
    then(annotationPublisherService).should().publishAnnotationNewSpecimen(Set.of());
    assertThat(result).isEmpty();
  }

  @Test
  void testUpdateSpecimenHandleFailed() throws Exception {
    var secondEvent = givenDigitalSpecimenEvent("Another Specimen");
    var thirdEvent = givenDigitalSpecimenEvent("A third Specimen");
    var events = List.of(givenDigitalSpecimenEvent(), secondEvent, thirdEvent);
    given(equalityService.isEqual(any(), any(), any())).willReturn(false);
    mockEqualityServiceSetDates(events);
    given(repository.getDigitalSpecimens(anyList()))
        .willReturn(List.of(givenDifferentUnequalSpecimen(THIRD_HANDLE, "A third Specimen"),
            givenDifferentUnequalSpecimen(SECOND_HANDLE, "Another Specimen"),
            givenUnequalDigitalSpecimenRecord()
        ));
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);
    doThrow(PidException.class).when(handleComponent).updateHandle(any());

    // When
    var result = service.handleMessages(events);

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
    var events = List.of(givenDigitalSpecimenEvent(), secondEvent, thirdEvent);
    given(repository.getDigitalSpecimens(anyList()))
        .willReturn(List.of(givenDifferentUnequalSpecimen(THIRD_HANDLE, "A third Specimen"),
            givenDifferentUnequalSpecimen(SECOND_HANDLE, "Another Specimen"),
            givenUnequalDigitalSpecimenRecord()
        ));
    given(equalityService.isEqual(any(), any(), any())).willReturn(false);
    mockEqualityServiceSetDates(events);
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);
    doThrow(PidException.class).when(handleComponent).updateHandle(any());
    doThrow(JsonProcessingException.class).when(kafkaService).deadLetterEvent(any());

    // When
    var result = service.handleMessages(events);

    // Then
    assertThat(result).isEmpty();
  }

  @Test
  void testUpdateSpecimenPartialElasticFailed() throws Exception {
    // Given
    var firstEvent = givenDigitalSpecimenEvent(true);
    var secondEvent = givenDigitalSpecimenEvent("Another Specimen");
    var thirdEvent = givenDigitalSpecimenEvent("A third Specimen");
    var secondRecord = givenDifferentUnequalSpecimen(SECOND_HANDLE, "Another Specimen");
    var thirdRecord = givenDifferentUnequalSpecimen(THIRD_HANDLE, "A third Specimen");
    var events = List.of(firstEvent, secondEvent, thirdEvent);
    given(equalityService.isEqual(any(), any(), any())).willReturn(false);
    mockEqualityServiceSetDates(events);
    given(repository.getDigitalSpecimens(
        List.of(PHYSICAL_SPECIMEN_ID, "A third Specimen", "Another Specimen")))
        .willReturn(List.of(
            givenUnequalDigitalSpecimenRecord(),
            thirdRecord,
            secondRecord));
    given(bulkResponse.errors()).willReturn(true);
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);
    given(elasticRepository.indexDigitalSpecimen(anyList())).willReturn(bulkResponse);
    given(midsService.calculateMids(firstEvent.digitalSpecimenWrapper())).willReturn(1);
    given(
        digitalMediaService.getExistingDigitalMedia(any(), anyList())).willReturn(
        givenMediaProcessResultMapNew(Map.of(
            THIRD_HANDLE, thirdEvent,
            SECOND_HANDLE, secondEvent,
            HANDLE, firstEvent
        )));
    given(rollbackService.handlePartiallyFailedElasticUpdate(any(), anyMap(), eq(bulkResponse)))
        .willReturn(Set.of(
            givenUpdatedDigitalSpecimenRecord(givenUnequalDigitalSpecimenRecord(), true),
            givenUpdatedDigitalSpecimenRecord(thirdRecord, false)
        ));

    // When
    var result = service.handleMessages(events);

    // Then
    then(handleComponent).should().updateHandle(anyList());
    then(repository).should(times(1)).createDigitalSpecimenRecord(anyList());
    then(kafkaService).should(times(1))
        .publishDigitalMediaObject(any(DigitalMediaEvent.class));
    then(kafkaService).shouldHaveNoMoreInteractions();
    assertThat(result).hasSize(2);
  }

  @Test
  void testUpdateSpecimenKafkaFailed() throws Exception {
    given(repository.getDigitalSpecimens(List.of(PHYSICAL_SPECIMEN_ID))).willReturn(
        List.of(givenUnequalDigitalSpecimenRecord()));
    given(midsService.calculateMids(givenDigitalSpecimenWrapper())).willReturn(1);
    given(equalityService.isEqual(any(), any(), any())).willReturn(false);
    mockEqualityServiceSetDates(List.of(givenDigitalSpecimenEvent()));
    given(bulkResponse.errors()).willReturn(false);
    given(
        elasticRepository.indexDigitalSpecimen(
            List.of(givenDigitalSpecimenRecord(2, false)))).willReturn(
        bulkResponse);
    doThrow(JsonProcessingException.class).when(kafkaService)
        .publishUpdateEvent(givenDigitalSpecimenRecord(2, false), givenJsonPatch());
    given(digitalMediaService.getExistingDigitalMedia(any(), anyList()))
        .willReturn(givenMediaProcessResultMapNew(Map.of(
            HANDLE, givenDigitalSpecimenEvent())));

    // When
    var result = service.handleMessages(List.of(givenDigitalSpecimenEvent()));

    // Then
    then(repository).should(times(1)).createDigitalSpecimenRecord(anyList());
    then(rollbackService).should().rollbackUpdatedSpecimens(any(), any(), eq(true), eq(true));
    then(kafkaService).shouldHaveNoMoreInteractions();
    assertThat(result).isEmpty();
  }

  @Test
  void testUpdateSpecimenIOException() throws Exception {
    // Given
    var unequalCurrentDigitalSpecimen = givenUnequalDigitalSpecimenRecord(ANOTHER_ORGANISATION);
    given(repository.getDigitalSpecimens(List.of(PHYSICAL_SPECIMEN_ID))).willReturn(
        List.of(unequalCurrentDigitalSpecimen));
    given(equalityService.isEqual(any(), any(), any())).willReturn(false);
    mockEqualityServiceSetDates(List.of(givenDigitalSpecimenEvent()));
    given(
        elasticRepository.indexDigitalSpecimen(
            List.of(givenDigitalSpecimenRecord(2, false)))).willThrow(
        IOException.class);
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);
    given(midsService.calculateMids(givenDigitalSpecimenWrapper())).willReturn(1);
    given(digitalMediaService.getExistingDigitalMedia(any(), anyList())).willReturn(
        givenEmptyMediaProcessResultMap());

    // When
    var result = service.handleMessages(List.of(givenDigitalSpecimenEvent()));

    // Then
    then(rollbackService).should().rollbackUpdatedSpecimens(any(), any(), eq(false), eq(true));
    then(fdoRecordService).should().buildUpdateHandleRequest(anyList());
    then(handleComponent).should().updateHandle(any());
    then(repository).should(times(1)).createDigitalSpecimenRecord(anyList());
    then(kafkaService).shouldHaveNoMoreInteractions();
    assertThat(result).isEmpty();
  }

  @Test
  void testNewSpecimenPidCreationException() throws Exception {
    // Given
    given(repository.getDigitalSpecimens(List.of(PHYSICAL_SPECIMEN_ID))).willReturn(List.of());
    given(fdoRecordService.buildPostHandleRequest(anyList())).willReturn(
        List.of(givenHandleRequest()));
    doThrow(PidException.class).when(handleComponent).postHandle(any());

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
    then(equalityService).shouldHaveNoInteractions();
    then(kafkaService).should().republishEvent(givenDigitalSpecimenEvent());
    then(kafkaService).shouldHaveNoMoreInteractions();
    then(fdoRecordService).shouldHaveNoInteractions();
  }

  @Test
  void testUpdateSpecimenThrowsDataAccessException() throws Exception {
    // Given
    var firstEvent = givenDigitalSpecimenEvent(true);
    var secondEvent = givenDigitalSpecimenEvent("Another Specimen");
    var thirdEvent = givenDigitalSpecimenEvent("A third Specimen");
    var events = List.of(firstEvent, secondEvent, thirdEvent);
    var firstRecord = givenUnequalDigitalSpecimenRecord();
    var secondRecord = givenDifferentUnequalSpecimen(SECOND_HANDLE, "Another Specimen");
    var thirdRecord = givenDifferentUnequalSpecimen(THIRD_HANDLE, "A third Specimen");
    var unequalOriginalSpecimens = List.of(
        thirdRecord,
        secondRecord,
        firstRecord
    );
    var mediaPidMap = (givenMediaProcessResultMapNew(Map.of(
        THIRD_HANDLE, thirdEvent,
        SECOND_HANDLE, secondEvent,
        HANDLE, firstEvent)));
    given(equalityService.isEqual(any(), any(), any())).willReturn(false);
    mockEqualityServiceSetDates(events);
    given(repository.getDigitalSpecimens(anyList()))
        .willReturn(unequalOriginalSpecimens);
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);
    given(midsService.calculateMids(firstEvent.digitalSpecimenWrapper())).willReturn(1);
    doThrow(DataAccessException.class).when(repository).createDigitalSpecimenRecord(anyList());
    given(digitalMediaService.getExistingDigitalMedia(any(), anyList()))
        .willReturn(mediaPidMap);

    // When
    var result = service.handleMessages(events);

    // Then
    then(rollbackService).should().rollbackUpdatedSpecimens(any(), any(), eq(false), eq(false));
    then(kafkaService).shouldHaveNoMoreInteractions();
    assertThat(result).isEmpty();
  }

  @Test
  void testNewSpecimenThrowsDataAccessException() throws Exception {
    // Given
    var newSpecimenEvent = givenDigitalSpecimenEvent(true);
    given(repository.getDigitalSpecimens(List.of(PHYSICAL_SPECIMEN_ID))).willReturn(List.of());
    given(midsService.calculateMids(givenDigitalSpecimenWrapper(true))).willReturn(1);
    given(
        fdoRecordService.buildPostHandleRequest(
            List.of(givenDigitalSpecimenWrapper(true)))).willReturn(
        List.of(MAPPER.createObjectNode()));
    given(handleComponent.postHandle(anyList()))
        .willReturn(givenHandleComponentResponse(List.of(givenDigitalSpecimenRecord())));
    doThrow(DataAccessException.class).when(repository).createDigitalSpecimenRecord(any());

    // When
    var result = service.handleMessages(List.of(newSpecimenEvent));

    // Then
    then(rollbackService).should().rollbackNewSpecimens(any(), any(), eq(false), eq(false));
    then(kafkaService).shouldHaveNoInteractions();
    assertThat(result).isEmpty();
  }

  private void mockEqualityServiceSetDates(List<DigitalSpecimenEvent> events) {
    for (var event : events) {
      given(equalityService.setEventDates(any(), eq(event))).willReturn(event);
    }

  }

}
