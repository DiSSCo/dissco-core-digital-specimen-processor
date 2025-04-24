package eu.dissco.core.digitalspecimenprocessor.service;

import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.ANOTHER_SPECIMEN_NAME;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.CREATED;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.HANDLE;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MAS;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MEDIA_PID;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MEDIA_URL;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.ORGANISATION_ID;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.PHYSICAL_SPECIMEN_ID;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.PHYSICAL_SPECIMEN_ID_ALT;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.SECOND_HANDLE;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalMediaEvent;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalMediaRecord;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenEvent;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenRecord;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenRecordWithMediaEr;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenWrapper;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenEmptyMediaProcessResult;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenHandleRequest;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenHandleResponseMedia;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenPidProcessResultMedia;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenPidProcessResultSpecimen;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenUnequalDigitalMediaRecord;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenUnequalDigitalSpecimenRecord;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenUpdatedDigitalMediaTuple;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenUpdatedDigitalSpecimenTuple;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.mockStatic;

import co.elastic.clients.elasticsearch.core.BulkResponse;
import eu.dissco.core.digitalspecimenprocessor.domain.relation.PidProcessResult;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenEvent;
import eu.dissco.core.digitalspecimenprocessor.exception.DisscoRepositoryException;
import eu.dissco.core.digitalspecimenprocessor.exception.PidException;
import eu.dissco.core.digitalspecimenprocessor.property.ApplicationProperties;
import eu.dissco.core.digitalspecimenprocessor.repository.DigitalMediaRepository;
import eu.dissco.core.digitalspecimenprocessor.repository.DigitalSpecimenRepository;
import eu.dissco.core.digitalspecimenprocessor.repository.ElasticSearchRepository;
import eu.dissco.core.digitalspecimenprocessor.utils.TestUtils;
import eu.dissco.core.digitalspecimenprocessor.web.HandleComponent;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
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
  private DigitalSpecimenRepository specimenRepository;
  @Mock
  private DigitalMediaRepository mediaRepository;
  @Mock
  private FdoRecordService fdoRecordService;
  @Mock
  private ElasticSearchRepository elasticRepository;
  @Mock
  private RabbitMqPublisherService publisherService;
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
  private EntityRelationshipService entityRelationshipService;
  @Mock
  private RollbackService rollbackService;
  @Mock
  private EqualityService equalityService;
  @Mock
  private DigitalSpecimenService digitalSpecimenService;

  private MockedStatic<Instant> mockedInstant;
  private MockedStatic<Clock> mockedClock;
  private ProcessingService service;

  @BeforeEach
  void setup() {
    service = new ProcessingService(specimenRepository, mediaRepository,
        digitalSpecimenService, digitalMediaService, entityRelationshipService, equalityService,
        publisherService, fdoRecordService, handleComponent);
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
  void testEqualSpecimenNoMedia() throws Exception {
    // Given
    given(specimenRepository.getDigitalSpecimens(List.of(PHYSICAL_SPECIMEN_ID))).willReturn(
        List.of(givenDigitalSpecimenRecord()));
    given(entityRelationshipService.processMediaRelationshipsForSpecimen(anyMap(), any(),
        anyMap())).willReturn(givenEmptyMediaProcessResult());
    given(equalityService.specimensAreEqual(any(), any(), any())).willReturn(true);
    given(mediaRepository.getExistingDigitalMedia(Set.of())).willReturn(List.of());

    // When
    var result = service.handleMessages(List.of(givenDigitalSpecimenEvent()));

    // Then
    assertThat(result).isEqualTo(List.of());
    then(digitalSpecimenService).should()
        .updateEqualSpecimen(List.of(givenDigitalSpecimenRecord()));
    then(digitalSpecimenService).shouldHaveNoMoreInteractions();
    then(handleComponent).shouldHaveNoInteractions();
    then(digitalMediaService).shouldHaveNoInteractions();
  }

  @Test
  void testEqualSpecimenNoMediaDuplicate() throws Exception {
    // Given
    given(specimenRepository.getDigitalSpecimens(List.of(PHYSICAL_SPECIMEN_ID))).willReturn(
        List.of(givenDigitalSpecimenRecord()));
    given(entityRelationshipService.processMediaRelationshipsForSpecimen(anyMap(), any(),
        anyMap())).willReturn(givenEmptyMediaProcessResult());
    given(equalityService.specimensAreEqual(any(), any(), any())).willReturn(true);
    given(mediaRepository.getExistingDigitalMedia(Set.of())).willReturn(List.of());

    // When
    var result = service.handleMessages(
        List.of(givenDigitalSpecimenEvent(), givenDigitalSpecimenEvent()));

    // Then
    assertThat(result).isEqualTo(List.of());
    then(digitalSpecimenService).should()
        .updateEqualSpecimen(List.of(givenDigitalSpecimenRecord()));
    then(digitalSpecimenService).shouldHaveNoMoreInteractions();
    then(handleComponent).shouldHaveNoInteractions();
    then(digitalMediaService).shouldHaveNoInteractions();
    then(publisherService).should().republishSpecimenEvent(givenDigitalSpecimenEvent());
  }

  @Test
  void testNewSpecimenNoMedia() throws Exception {
    given(specimenRepository.getDigitalSpecimens(List.of(PHYSICAL_SPECIMEN_ID))).willReturn(
        List.of());
    given(mediaRepository.getExistingDigitalMedia(Set.of())).willReturn(List.of());
    given(handleComponent.postHandle(any(), eq(true))).willReturn(
        TestUtils.givenHandleResponseSpecimen());
    given(fdoRecordService.buildPostHandleRequest(any())).willReturn(List.of(givenHandleRequest()));
    var pidMap = Map.of(PHYSICAL_SPECIMEN_ID, givenPidProcessResultSpecimen(false));

    // When
    var result = service.handleMessages(List.of(givenDigitalSpecimenEvent()));

    // Then
    assertThat(result).isEqualTo(List.of());
    then(digitalSpecimenService).should()
        .createNewDigitalSpecimen(List.of(givenDigitalSpecimenEvent()), pidMap);
    then(equalityService).shouldHaveNoInteractions();
    then(digitalSpecimenService).shouldHaveNoMoreInteractions();
    then(handleComponent).shouldHaveNoMoreInteractions();
    then(digitalMediaService).shouldHaveNoInteractions();
  }

  @Test
  void testNewSpecimenNoMediaPidFails() throws Exception {
    given(specimenRepository.getDigitalSpecimens(List.of(PHYSICAL_SPECIMEN_ID))).willReturn(
        List.of());
    given(mediaRepository.getExistingDigitalMedia(Set.of())).willReturn(List.of());
    given(fdoRecordService.buildPostHandleRequest(any())).willReturn(List.of(givenHandleRequest()));
    given(handleComponent.postHandle(any(), eq(true))).willThrow(PidException.class);

    // When
    var result = service.handleMessages(List.of(givenDigitalSpecimenEvent()));

    // Then
    assertThat(result).isEqualTo(List.of());
    then(digitalSpecimenService).shouldHaveNoInteractions();
    then(equalityService).shouldHaveNoInteractions();
    then(handleComponent).shouldHaveNoMoreInteractions();
    then(digitalMediaService).shouldHaveNoInteractions();
  }

  @Test
  void testChangedSpecimenNoMedia() throws Exception {
    given(specimenRepository.getDigitalSpecimens(List.of(PHYSICAL_SPECIMEN_ID))).willReturn(
        List.of(givenUnequalDigitalSpecimenRecord()));
    given(equalityService.specimensAreEqual(any(), any(), any())).willReturn(false);
    given(mediaRepository.getExistingDigitalMedia(Set.of())).willReturn(List.of());
    given(equalityService.setEventDatesSpecimen(any(), any())).willReturn(
        givenDigitalSpecimenEvent());
    given(entityRelationshipService.processMediaRelationshipsForSpecimen(any(), any(),
        any())).willReturn(givenEmptyMediaProcessResult());
    var pidMap = Map.of(PHYSICAL_SPECIMEN_ID, givenPidProcessResultSpecimen(false));

    // When
    var result = service.handleMessages(List.of(givenDigitalSpecimenEvent()));

    // Then
    assertThat(result).isEqualTo(List.of());
    then(digitalSpecimenService).should()
        .updateExistingDigitalSpecimen(List.of(givenUpdatedDigitalSpecimenTuple(false, givenEmptyMediaProcessResult())), pidMap);
    then(equalityService).shouldHaveNoMoreInteractions();
    then(digitalSpecimenService).shouldHaveNoMoreInteractions();
    then(handleComponent).shouldHaveNoInteractions();
    then(fdoRecordService).shouldHaveNoInteractions();
    then(digitalMediaService).shouldHaveNoInteractions();
  }

  @Test
  void testHandleMessagesDbException() throws Exception {
    // Given
    given(specimenRepository.getDigitalSpecimens(any())).willThrow(DisscoRepositoryException.class);

    // When
    var result = service.handleMessages(List.of(givenDigitalSpecimenEvent()));

    // Then
    assertThat(result).isEmpty();
    then(digitalMediaService).shouldHaveNoInteractions();
    then(digitalSpecimenService).shouldHaveNoInteractions();
  }

  @Test
  void testEqualSpecimenWithEqualMedia() throws Exception {
    // Given
    given(specimenRepository.getDigitalSpecimens(List.of(PHYSICAL_SPECIMEN_ID))).willReturn(
        List.of(givenDigitalSpecimenRecord(1, true)));
    given(mediaRepository.getExistingDigitalMedia(Set.of(MEDIA_URL))).willReturn(
        List.of(givenDigitalMediaRecord()));
    given(entityRelationshipService.processMediaRelationshipsForSpecimen(anyMap(), any(),
        anyMap())).willReturn(givenEmptyMediaProcessResult());
    given(entityRelationshipService.findNewSpecimenRelationshipsForMedia(any(), any())).willReturn(
        Set.of());
    given(equalityService.specimensAreEqual(any(), any(), any())).willReturn(true);
    given(equalityService.mediaAreEqual(any(), any(), any())).willReturn(true);

    // When
    service.handleMessages(List.of(givenDigitalSpecimenEvent(true)));

    // Then
    then(digitalSpecimenService).should()
        .updateEqualSpecimen(List.of(givenDigitalSpecimenRecord(1, true)));
    then(digitalMediaService).should().processEqualDigitalMedia(List.of(givenDigitalMediaRecord()));
    then(digitalSpecimenService).shouldHaveNoMoreInteractions();
    then(digitalMediaService).shouldHaveNoMoreInteractions();
    then(handleComponent).shouldHaveNoInteractions();
  }

  @Test
  void testEqualSpecimenWithEqualDuplicateMedia() throws Exception {
    // Given
    given(specimenRepository.getDigitalSpecimens(List.of(PHYSICAL_SPECIMEN_ID))).willReturn(
        List.of(givenDigitalSpecimenRecord(1, true)));
    given(mediaRepository.getExistingDigitalMedia(Set.of(MEDIA_URL))).willReturn(
        List.of(givenDigitalMediaRecord()));
    given(entityRelationshipService.processMediaRelationshipsForSpecimen(anyMap(), any(),
        anyMap())).willReturn(givenEmptyMediaProcessResult());
    given(entityRelationshipService.findNewSpecimenRelationshipsForMedia(any(), any())).willReturn(
        Set.of());
    given(equalityService.specimensAreEqual(any(), any(), any())).willReturn(true);
    given(equalityService.mediaAreEqual(any(), any(), any())).willReturn(true);
    var event = new DigitalSpecimenEvent(
        List.of(MAS),
        givenDigitalSpecimenWrapper(false, true),
        List.of(givenDigitalMediaEvent(), givenDigitalMediaEvent())
    );

    // When
    service.handleMessages(List.of(event));

    // Then
    then(digitalSpecimenService).should()
        .updateEqualSpecimen(List.of(givenDigitalSpecimenRecord(1, true)));
    then(digitalMediaService).should().processEqualDigitalMedia(List.of(givenDigitalMediaRecord()));
    then(digitalSpecimenService).shouldHaveNoMoreInteractions();
    then(digitalMediaService).shouldHaveNoMoreInteractions();
    then(handleComponent).shouldHaveNoInteractions();
    then(publisherService).should().republishMediaEvent(givenDigitalMediaEvent());
  }

  @Test
  void testChangedSpecimenUpdatedMedia() throws Exception {
    given(specimenRepository.getDigitalSpecimens(List.of(PHYSICAL_SPECIMEN_ID))).willReturn(
        List.of(givenUnequalDigitalSpecimenRecord(HANDLE, ANOTHER_SPECIMEN_NAME, ORGANISATION_ID,
            true)));
    given(equalityService.specimensAreEqual(any(), any(), any())).willReturn(false);
    given(mediaRepository.getExistingDigitalMedia(any())).willReturn(
        List.of(givenUnequalDigitalMediaRecord()));
    given(equalityService.setEventDatesSpecimen(any(), any())).willReturn(
        givenDigitalSpecimenEvent(true));
    given(equalityService.setEventDatesMedia(any(), any())).willReturn(givenDigitalMediaEvent());
    given(entityRelationshipService.processMediaRelationshipsForSpecimen(any(), any(),
        any())).willReturn(givenEmptyMediaProcessResult());
    var pidMapSpecimen = Map.of(PHYSICAL_SPECIMEN_ID, givenPidProcessResultSpecimen(true));
    var pidMapMedia = Map.of(MEDIA_URL, givenPidProcessResultMedia());

    // When
    service.handleMessages(List.of(givenDigitalSpecimenEvent(true)));

    // Then
    then(digitalSpecimenService).should()
        .updateExistingDigitalSpecimen(List.of(givenUpdatedDigitalSpecimenTuple(true, givenEmptyMediaProcessResult())),
            pidMapSpecimen);
    then(digitalMediaService).should()
        .updateExistingDigitalMedia(List.of(givenUpdatedDigitalMediaTuple(false)), pidMapMedia);
    then(digitalSpecimenService).shouldHaveNoMoreInteractions();
    then(handleComponent).shouldHaveNoInteractions();
    then(fdoRecordService).shouldHaveNoInteractions();
    then(digitalMediaService).shouldHaveNoMoreInteractions();
  }

  @Test
  void testChangedSpecimenUpdatedMediaSharedMedia() throws Exception {
    var event1 = givenDigitalSpecimenEvent(true);
    var event2 = givenDigitalSpecimenEvent(PHYSICAL_SPECIMEN_ID_ALT);
    var record1 = givenDigitalSpecimenRecordWithMediaEr();
    var record2 = givenDigitalSpecimenRecordWithMediaEr(SECOND_HANDLE, PHYSICAL_SPECIMEN_ID_ALT, false);
    given(specimenRepository.getDigitalSpecimens(any())).willReturn(
        List.of(record1, record2));
    given(equalityService.specimensAreEqual(any(), any(), any())).willReturn(false);
    given(mediaRepository.getExistingDigitalMedia(Set.of(MEDIA_URL))).willReturn(
        List.of(givenUnequalDigitalMediaRecord()));
    given(equalityService.setEventDatesSpecimen(any(), eq(event1))).willReturn(
        event1);
    given(equalityService.setEventDatesSpecimen(any(), eq(event2))).willReturn(
        event2);
    given(equalityService.setEventDatesMedia(any(), any())).willReturn(givenDigitalMediaEvent());

    given(entityRelationshipService.processMediaRelationshipsForSpecimen(any(), any(),
        any())).willReturn(givenEmptyMediaProcessResult());
    var pidMapSpecimen = Map.of(
        PHYSICAL_SPECIMEN_ID, givenPidProcessResultSpecimen(true),
        PHYSICAL_SPECIMEN_ID_ALT, new PidProcessResult(SECOND_HANDLE, Set.of(MEDIA_PID)));
    var pidMapMedia = Map.of(MEDIA_URL, new PidProcessResult(MEDIA_PID, Set.of(HANDLE, SECOND_HANDLE)));

    // When
    service.handleMessages(List.of(event1, event2));

    // Then
    then(digitalSpecimenService).should()
        .updateExistingDigitalSpecimen(any(), eq(pidMapSpecimen));
    then(digitalMediaService).should()
        .updateExistingDigitalMedia(List.of(givenUpdatedDigitalMediaTuple(false)), pidMapMedia);
    then(digitalSpecimenService).shouldHaveNoMoreInteractions();
    then(digitalMediaService).shouldHaveNoMoreInteractions();
    then(handleComponent).shouldHaveNoInteractions();
    then(fdoRecordService).shouldHaveNoInteractions();
  }

  @Test
  void testNewSpecimenNewMedia() throws Exception {
    given(specimenRepository.getDigitalSpecimens(List.of(PHYSICAL_SPECIMEN_ID))).willReturn(
        List.of());
    given(mediaRepository.getExistingDigitalMedia(Set.of(MEDIA_URL))).willReturn(List.of());
    given(handleComponent.postHandle(any(), eq(true))).willReturn(
        TestUtils.givenHandleResponseSpecimen());
    given(handleComponent.postHandle(any(), eq(false))).willReturn(givenHandleResponseMedia());
    given(fdoRecordService.buildPostHandleRequest(any())).willReturn(List.of(givenHandleRequest()));
    given(fdoRecordService.buildPostRequestMedia(any())).willReturn(List.of(givenHandleRequest()));
    var pidMap = Map.of(PHYSICAL_SPECIMEN_ID, givenPidProcessResultSpecimen(true));
    var pidMapMedia = Map.of(MEDIA_URL, givenPidProcessResultMedia());

    // When
    var result = service.handleMessages(List.of(givenDigitalSpecimenEvent(true)));

    // Then
    assertThat(result).isEqualTo(List.of());
    then(digitalSpecimenService).should()
        .createNewDigitalSpecimen(List.of(givenDigitalSpecimenEvent(true)), pidMap);
    then(digitalMediaService).should().createNewDigitalMedia(List.of(givenDigitalMediaEvent()), pidMapMedia);
    then(equalityService).shouldHaveNoInteractions();
    then(digitalSpecimenService).shouldHaveNoMoreInteractions();
    then(digitalMediaService).shouldHaveNoMoreInteractions();
  }







  /*
  @Test
  void testEqualSpecimen() throws DisscoRepositoryException, JsonProcessingException {
    // Given
    given(repository.getDigitalSpecimens(List.of(PHYSICAL_SPECIMEN_ID))).willReturn(
        List.of(givenDigitalSpecimenWithEntityRelationship()));
    given(equalityService.specimensAreEqual(any(), any(), any())).willReturn(true);
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
    then(rabbitMQService).should(times(1))
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
    given(equalityService.specimensAreEqual(any(), any(),
        eq(givenEmptyMediaProcessResult()))).willReturn(
        true);

    // When
    List<DigitalSpecimenRecord> result = service.handleMessages(
        List.of(givenDigitalSpecimenEvent(false, false)));

    // Then
    verifyNoInteractions(handleComponent);
    verifyNoInteractions(fdoRecordService);
    verifyNoInteractions(annotationPublisherService);
    then(repository).should().updateLastChecked(List.of(HANDLE));
    then(rabbitMQService).shouldHaveNoInteractions();
    assertThat(result).isEmpty();
  }

  @Test
  void testUnequalSpecimen() throws Exception {
    // Given
    var currentSpecimenRecord = new DigitalSpecimenRecord(HANDLE, MIDS_LEVEL, VERSION, CREATED,
        new DigitalSpecimenWrapper(PHYSICAL_SPECIMEN_ID, ANOTHER_SPECIMEN_NAME,
            new DigitalSpecimen().withOdsTopicDiscipline(OdsTopicDiscipline.ECOLOGY), null));
    var expected = List.of(givenDigitalSpecimenRecord(2, true));
    given(equalityService.specimensAreEqual(any(), any(), any())).willReturn(false);
    mockEqualityServiceSetDates(List.of(givenDigitalSpecimenEvent(true)));
    given(repository.getDigitalSpecimens(List.of(PHYSICAL_SPECIMEN_ID))).willReturn(
        List.of(currentSpecimenRecord));
    given(bulkResponse.errors()).willReturn(false);
    given(elasticRepository.indexDigitalSpecimen(expected)).willReturn(bulkResponse);
    given(midsService.calculateMids(givenDigitalSpecimenWrapper(true))).willReturn(1);
    given(fdoRecordService.handleNeedsUpdateSpecimen(any(), any())).willReturn(true);
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
    then(rabbitMQService).should()
        .publishUpdateEventSpecimen(eq(givenDigitalSpecimenRecord(2, true)), any(JsonNode.class));
    then(rabbitMQService).should()
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
    var newMediaResult = new MediaRelationshipProcessResult(List.of(), currentMediaEvent);
    given(equalityService.specimensAreEqual(any(), any(), eq(newMediaResult))).willReturn(false);
    mockEqualityServiceSetDates(List.of(givenDigitalSpecimenEvent(true)));
    given(repository.getDigitalSpecimens(List.of(PHYSICAL_SPECIMEN_ID))).willReturn(
        List.of(currentSpecimenRecord));
    given(bulkResponse.errors()).willReturn(false);
    given(elasticRepository.indexDigitalSpecimen(expected)).willReturn(bulkResponse);
    given(midsService.calculateMids(givenDigitalSpecimenWrapper(true))).willReturn(1);
    given(fdoRecordService.handleNeedsUpdateSpecimen(any(), any())).willReturn(true);
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
    then(rabbitMQService).should()
        .publishUpdateEventSpecimen(eq(expected.get(0)), any(JsonNode.class));
    then(rabbitMQService).should()
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
    var tombstoneResult = new MediaRelationshipProcessResult(mediaEr, List.of());
    var expected = List.of(givenDigitalSpecimenRecord(2, true));
    given(equalityService.specimensAreEqual(any(), any(), eq(tombstoneResult))).willReturn(false);
    mockEqualityServiceSetDates(List.of(givenDigitalSpecimenEvent(true)));
    given(repository.getDigitalSpecimens(List.of(PHYSICAL_SPECIMEN_ID))).willReturn(
        List.of(currentSpecimenRecord));
    given(bulkResponse.errors()).willReturn(false);
    given(elasticRepository.indexDigitalSpecimen(expected)).willReturn(bulkResponse);
    given(midsService.calculateMids(givenDigitalSpecimenWrapper(true))).willReturn(1);
    given(fdoRecordService.handleNeedsUpdateSpecimen(any(), any())).willReturn(true);
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
    then(rabbitMQService).should()
        .publishUpdateEventSpecimen(eq(givenDigitalSpecimenRecord(2, true)), any(JsonNode.class));
    then(rabbitMQService).should()
        .publishDigitalMediaObject(givenDigitalMediaEventWithRelationship());
    assertThat(result).isEqualTo(List.of(givenDigitalSpecimenRecord(2, true)));
    then(digitalMediaService).should().removeSpecimenRelationshipsFromMedia(any());
    then(annotationPublisherService).should().publishAnnotationUpdatedSpecimen(anySet());
  }

  @Test
  void testHandleRecordDoesNotNeedUpdate() throws Exception {
    // Given
    var expected = List.of(givenDigitalSpecimenRecord(2, true));
    given(equalityService.specimensAreEqual(any(), any(), any())).willReturn(false);
    mockEqualityServiceSetDates(List.of(givenDigitalSpecimenEvent(true)));
    given(repository.getDigitalSpecimens(List.of(PHYSICAL_SPECIMEN_ID))).willReturn(
        List.of(givenUnequalDigitalSpecimenRecord(HANDLE, ANOTHER_SPECIMEN_NAME, ORGANISATION_ID,
            true)));
    given(bulkResponse.errors()).willReturn(false);
    given(elasticRepository.indexDigitalSpecimen(expected)).willReturn(bulkResponse);
    given(midsService.calculateMids(givenDigitalSpecimenWrapper(true))).willReturn(1);
    given(fdoRecordService.handleNeedsUpdateSpecimen(any(), any())).willReturn(false);
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
    then(rabbitMQService).should()
        .publishUpdateEventSpecimen(givenDigitalSpecimenRecord(2, true), givenJsonPatch());
    then(rabbitMQService).should()
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
    given(handleComponent.postHandle(anyList(), true))
        .willReturn(givenHandleComponentResponse(List.of(givenDigitalSpecimenRecord())));
    given(handleComponent.postMediaHandle(anyList()))
        .willReturn(givenMediaPidResponse());
    given(fdoRecordService.buildPostRequestMedia(any())).willReturn(
        List.of(MAPPER.createObjectNode()));
    given(applicationProperties.getPid()).willReturn(APP_HANDLE);
    given(applicationProperties.getName()).willReturn(APP_NAME);

    // When
    var result = service.handleMessages(List.of(givenDigitalSpecimenEvent(true)));

    // Then
    then(equalityService).shouldHaveNoInteractions();
    then(repository).should()
        .createDigitalSpecimenRecord(Set.of(givenDigitalSpecimenRecordWithMediaEr()));
    then(rabbitMQService).should().publishCreateEventSpecimen(givenDigitalSpecimenRecordWithMediaEr());
    then(rabbitMQService).should()
        .publishAnnotationRequestEventSpecimen(MAS, givenDigitalSpecimenRecordWithMediaEr());
    then(rabbitMQService).should()
        .publishDigitalMediaObject(givenDigitalMediaEventWithRelationship(MEDIA_PID));
    assertThat(result).isEqualTo(List.of(givenDigitalSpecimenRecordWithMediaEr()));
    then(annotationPublisherService).should()
        .publishAnnotationNewSpecimen(Set.of(givenDigitalSpecimenRecordWithMediaEr()));
  }

  @Test
  void testNewSpecimenPidFailed() throws Exception {
    // Given
    given(repository.getDigitalSpecimens(List.of(PHYSICAL_SPECIMEN_ID))).willReturn(List.of());
    given(handleComponent.postHandle(anyList(), true)).willThrow(PidException.class);
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
    given(handleComponent.postHandle(anyList(), true))
        .willReturn(givenHandleComponentResponse(List.of(givenDigitalSpecimenRecord())));

    // When
    var result = service.handleMessages(
        List.of(givenDigitalSpecimenEvent(), duplicateSpecimen));

    // Then
    then(equalityService).shouldHaveNoInteractions();
    verify(handleComponent, times(1)).postHandle(anyList(),
        true);
    then(repository).should().createDigitalSpecimenRecord(Set.of(givenDigitalSpecimenRecord()));
    then(rabbitMQService).should().publishCreateEventSpecimen(givenDigitalSpecimenRecord());
    then(rabbitMQService).should().publishAnnotationRequestEventSpecimen(MAS, givenDigitalSpecimenRecord());
    then(rabbitMQService).should().republishSpecimenEvent(duplicateSpecimen);
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
    given(handleComponent.postHandle(anyList(), true))
        .willReturn(givenHandleComponentResponse(List.of(givenDigitalSpecimenRecord())));

    // When
    var result = service.handleMessages(List.of(givenDigitalSpecimenEvent()));

    // Then
    then(equalityService).shouldHaveNoInteractions();
    then(rollbackService).should()
        .rollbackNewSpecimens(anyMap(), eq(false), eq(true));
    then(rabbitMQService).shouldHaveNoInteractions();
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
    given(fdoRecordService.buildPostRequestMedia(any())).willReturn(
        List.of(MAPPER.createObjectNode()));
    given(handleComponent.postHandle(anyList(), true))
        .willReturn(Map.of(
            PHYSICAL_SPECIMEN_ID, HANDLE,
            secondPhysicalId, SECOND_HANDLE,
            thirdPhysicalId, THIRD_HANDLE
        ));
    given(handleComponent.postMediaHandle(any())).willReturn(Map.of(
        new DigitalMediaKey(HANDLE, MEDIA_URL), MEDIA_PID_ALT,
        new DigitalMediaKey(SECOND_HANDLE, MEDIA_URL), MEDIA_PID,
        new DigitalMediaKey(THIRD_HANDLE, MEDIA_URL), MEDIA_PID));
    given(rollbackService.handlePartiallyFailedElasticInsertSpecimen(any(), any(), eq(bulkResponse)))
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
    then(rabbitMQService).should()
        .publishDigitalMediaObject(any(DigitalMediaEvent.class));
    then(rabbitMQService).shouldHaveNoMoreInteractions();
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
    given(handleComponent.postHandle(anyList(), true))
        .willReturn(givenHandleComponentResponse(List.of(givenDigitalSpecimenRecord())));
    given(bulkResponse.errors()).willReturn(false);
    given(
        elasticRepository.indexDigitalSpecimen(Set.of(givenDigitalSpecimenRecord()))).willReturn(
        bulkResponse);
    doThrow(JsonProcessingException.class).when(rabbitMQService)
        .publishCreateEventSpecimen(any(DigitalSpecimenRecord.class));
    given(midsService.calculateMids(givenDigitalSpecimenWrapper())).willReturn(1);

    // When
    var result = service.handleMessages(List.of(givenDigitalSpecimenEvent()));

    // Then
    then(equalityService).shouldHaveNoInteractions();
    then(repository).should().createDigitalSpecimenRecord(anySet());
    then(rollbackService).should().rollbackNewSpecimens(any(), eq(true), eq(true));
    then(rabbitMQService).shouldHaveNoMoreInteractions();
    then(annotationPublisherService).should().publishAnnotationNewSpecimen(Set.of());
    assertThat(result).isEmpty();
  }

  @Test
  void testUpdateSpecimenHandleFailed() throws Exception {
    var secondEvent = givenDigitalSpecimenEvent("Another Specimen");
    var thirdEvent = givenDigitalSpecimenEvent("A third Specimen");
    var events = List.of(givenDigitalSpecimenEvent(), secondEvent, thirdEvent);
    given(equalityService.specimensAreEqual(any(), any(), any())).willReturn(false);
    mockEqualityServiceSetDates(events);
    given(repository.getDigitalSpecimens(anyList()))
        .willReturn(List.of(givenDifferentUnequalSpecimen(THIRD_HANDLE, "A third Specimen"),
            givenDifferentUnequalSpecimen(SECOND_HANDLE, "Another Specimen"),
            givenUnequalDigitalSpecimenRecord()
        ));
    given(fdoRecordService.handleNeedsUpdateSpecimen(any(), any())).willReturn(true);
    doThrow(PidException.class).when(handleComponent).updateHandle(any());

    // When
    var result = service.handleMessages(events);

    // Then
    then(rabbitMQService).should().deadLetterEventSpecimen(givenDigitalSpecimenEvent());
    then(rabbitMQService).should().deadLetterEventSpecimen(secondEvent);
    then(rabbitMQService).should().deadLetterEventSpecimen(thirdEvent);
    then(rabbitMQService).shouldHaveNoMoreInteractions();
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
    given(equalityService.specimensAreEqual(any(), any(), any())).willReturn(false);
    mockEqualityServiceSetDates(events);
    given(fdoRecordService.handleNeedsUpdateSpecimen(any(), any())).willReturn(true);
    doThrow(PidException.class).when(handleComponent).updateHandle(any());
    doThrow(JsonProcessingException.class).when(rabbitMQService).deadLetterEventSpecimen(any());

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
    given(equalityService.specimensAreEqual(any(), any(), any())).willReturn(false);
    mockEqualityServiceSetDates(events);
    given(repository.getDigitalSpecimens(
        List.of(PHYSICAL_SPECIMEN_ID, "A third Specimen", "Another Specimen")))
        .willReturn(List.of(
            givenUnequalDigitalSpecimenRecord(),
            thirdRecord,
            secondRecord));
    given(bulkResponse.errors()).willReturn(true);
    given(fdoRecordService.handleNeedsUpdateSpecimen(any(), any())).willReturn(true);
    given(elasticRepository.indexDigitalSpecimen(anyList())).willReturn(bulkResponse);
    given(midsService.calculateMids(firstEvent.digitalSpecimenWrapper())).willReturn(1);
    given(
        digitalMediaService.getExistingDigitalMedia(any(), anyList())).willReturn(
        givenMediaProcessResultMapNew(Map.of(
            THIRD_HANDLE, thirdEvent,
            SECOND_HANDLE, secondEvent,
            HANDLE, firstEvent
        )));
    given(rollbackService.handlePartiallyFailedElasticUpdateSpecimen(any(), eq(bulkResponse)))
        .willReturn(Set.of(
            givenUpdatedDigitalSpecimenRecord(givenUnequalDigitalSpecimenRecord(), true),
            givenUpdatedDigitalSpecimenRecord(thirdRecord, false)
        ));

    // When
    var result = service.handleMessages(events);

    // Then
    then(handleComponent).should().updateHandle(anyList());
    then(repository).should(times(1)).createDigitalSpecimenRecord(anyList());
    then(rabbitMQService).should(times(1))
        .publishDigitalMediaObject(any(DigitalMediaEvent.class));
    then(rabbitMQService).shouldHaveNoMoreInteractions();
    assertThat(result).hasSize(2);
  }

  @Test
  void testUpdateSpecimenKafkaFailed() throws Exception {
    given(repository.getDigitalSpecimens(List.of(PHYSICAL_SPECIMEN_ID))).willReturn(
        List.of(givenUnequalDigitalSpecimenRecord()));
    given(midsService.calculateMids(givenDigitalSpecimenWrapper())).willReturn(1);
    given(equalityService.specimensAreEqual(any(), any(), any())).willReturn(false);
    mockEqualityServiceSetDates(List.of(givenDigitalSpecimenEvent()));
    given(bulkResponse.errors()).willReturn(false);
    given(
        elasticRepository.indexDigitalSpecimen(
            List.of(givenDigitalSpecimenRecord(2, false)))).willReturn(
        bulkResponse);
    doThrow(JsonProcessingException.class).when(rabbitMQService)
        .publishUpdateEventSpecimen(givenDigitalSpecimenRecord(2, false), givenJsonPatch());
    given(digitalMediaService.getExistingDigitalMedia(any(), anyList()))
        .willReturn(givenMediaProcessResultMapNew(Map.of(
            HANDLE, givenDigitalSpecimenEvent())));

    // When
    var result = service.handleMessages(List.of(givenDigitalSpecimenEvent()));

    // Then
    then(repository).should(times(1)).createDigitalSpecimenRecord(anyList());
    then(rollbackService).should().rollbackUpdatedSpecimens(any(), eq(true), eq(true));
    then(rabbitMQService).shouldHaveNoMoreInteractions();
    assertThat(result).isEmpty();
  }

  @Test
  void testUpdateSpecimenIOException() throws Exception {
    // Given
    var unequalCurrentDigitalSpecimen = givenUnequalDigitalSpecimenRecord(ANOTHER_ORGANISATION);
    given(repository.getDigitalSpecimens(List.of(PHYSICAL_SPECIMEN_ID))).willReturn(
        List.of(unequalCurrentDigitalSpecimen));
    given(equalityService.specimensAreEqual(any(), any(), any())).willReturn(false);
    mockEqualityServiceSetDates(List.of(givenDigitalSpecimenEvent()));
    given(
        elasticRepository.indexDigitalSpecimen(
            List.of(givenDigitalSpecimenRecord(2, false)))).willThrow(
        IOException.class);
    given(fdoRecordService.handleNeedsUpdateSpecimen(any(), any())).willReturn(true);
    given(midsService.calculateMids(givenDigitalSpecimenWrapper())).willReturn(1);
    given(digitalMediaService.getExistingDigitalMedia(any(), anyList())).willReturn(
        givenEmptyMediaProcessResultMap());

    // When
    var result = service.handleMessages(List.of(givenDigitalSpecimenEvent()));

    // Then
    then(rollbackService).should().rollbackUpdatedSpecimens(any(), eq(false), eq(true));
    then(fdoRecordService).should().buildUpdateHandleRequest(anyList());
    then(handleComponent).should().updateHandle(any());
    then(repository).should(times(1)).createDigitalSpecimenRecord(anyList());
    then(rabbitMQService).shouldHaveNoMoreInteractions();
    assertThat(result).isEmpty();
  }

  @Test
  void testNewSpecimenPidCreationException() throws Exception {
    // Given
    given(repository.getDigitalSpecimens(List.of(PHYSICAL_SPECIMEN_ID))).willReturn(List.of());
    given(fdoRecordService.buildPostHandleRequest(anyList())).willReturn(
        List.of(givenHandleRequest()));
    doThrow(PidException.class).when(handleComponent).postHandle(any(), true);

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
    then(rabbitMQService).should().republishSpecimenEvent(givenDigitalSpecimenEvent());
    then(rabbitMQService).shouldHaveNoMoreInteractions();
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
    given(equalityService.specimensAreEqual(any(), any(), any())).willReturn(false);
    mockEqualityServiceSetDates(events);
    given(repository.getDigitalSpecimens(anyList()))
        .willReturn(unequalOriginalSpecimens);
    given(fdoRecordService.handleNeedsUpdateSpecimen(any(), any())).willReturn(true);
    given(midsService.calculateMids(firstEvent.digitalSpecimenWrapper())).willReturn(1);
    doThrow(DataAccessException.class).when(repository).createDigitalSpecimenRecord(anyList());
    given(digitalMediaService.getExistingDigitalMedia(any(), anyList()))
        .willReturn(mediaPidMap);

    // When
    var result = service.handleMessages(events);

    // Then
    then(rollbackService).should().rollbackUpdatedSpecimens(any(), eq(false), eq(false));
    then(rabbitMQService).shouldHaveNoMoreInteractions();
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
    given(handleComponent.postHandle(anyList(), true))
        .willReturn(givenHandleComponentResponse(List.of(givenDigitalSpecimenRecord())));
    doThrow(DataAccessException.class).when(repository).createDigitalSpecimenRecord(any());

    // When
    var result = service.handleMessages(List.of(newSpecimenEvent));

    // Then
    then(rollbackService).should().rollbackNewSpecimens(any(), eq(false), eq(false));
    then(rabbitMQService).shouldHaveNoInteractions();
    assertThat(result).isEmpty();
  }

  private void mockEqualityServiceSetDates(List<DigitalSpecimenEvent> events) {
    for (var event : events) {
      given(equalityService.setEventDatesSpecimen(any(), eq(event))).willReturn(event);
    }

  }
   */

}
