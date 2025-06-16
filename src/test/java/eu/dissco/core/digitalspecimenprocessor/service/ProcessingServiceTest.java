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
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;

import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaEvent;
import eu.dissco.core.digitalspecimenprocessor.domain.relation.PidProcessResult;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenEvent;
import eu.dissco.core.digitalspecimenprocessor.exception.DisscoRepositoryException;
import eu.dissco.core.digitalspecimenprocessor.exception.PidException;
import eu.dissco.core.digitalspecimenprocessor.exception.TooManyObjectsException;
import eu.dissco.core.digitalspecimenprocessor.property.ApplicationProperties;
import eu.dissco.core.digitalspecimenprocessor.repository.DigitalMediaRepository;
import eu.dissco.core.digitalspecimenprocessor.repository.DigitalSpecimenRepository;
import eu.dissco.core.digitalspecimenprocessor.utils.TestUtils;
import eu.dissco.core.digitalspecimenprocessor.web.HandleComponent;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.IntStream;
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
  private RabbitMqPublisherService publisherService;
  @Mock
  private HandleComponent handleComponent;
  @Mock
  private ApplicationProperties applicationProperties;
  @Mock
  private DigitalMediaService digitalMediaService;
  @Mock
  private EntityRelationshipService entityRelationshipService;
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
        publisherService, fdoRecordService, handleComponent, new ApplicationProperties());
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
    given(equalityService.setExistingEventDatesSpecimen(any(), any(), any())).willReturn(
        givenDigitalSpecimenEvent());
    given(entityRelationshipService.processMediaRelationshipsForSpecimen(any(), any(),
        any())).willReturn(givenEmptyMediaProcessResult());
    var pidMap = Map.of(PHYSICAL_SPECIMEN_ID, givenPidProcessResultSpecimen(false));

    // When
    var result = service.handleMessages(List.of(givenDigitalSpecimenEvent()));

    // Then
    assertThat(result).isEqualTo(List.of());
    then(digitalSpecimenService).should()
        .updateExistingDigitalSpecimen(
            List.of(givenUpdatedDigitalSpecimenTuple(false, givenEmptyMediaProcessResult())),
            pidMap);
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
    then(digitalMediaService).should().updateEqualDigitalMedia(List.of(givenDigitalMediaRecord()));
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
    then(digitalMediaService).should().updateEqualDigitalMedia(List.of(givenDigitalMediaRecord()));
    then(digitalSpecimenService).shouldHaveNoMoreInteractions();
    then(digitalMediaService).shouldHaveNoMoreInteractions();
    then(handleComponent).shouldHaveNoInteractions();
  }

  @Test
  void testChangedSpecimenUpdatedMedia() throws Exception {
    given(specimenRepository.getDigitalSpecimens(List.of(PHYSICAL_SPECIMEN_ID))).willReturn(
        List.of(givenUnequalDigitalSpecimenRecord(HANDLE, ANOTHER_SPECIMEN_NAME, ORGANISATION_ID,
            true)));
    given(equalityService.specimensAreEqual(any(), any(), any())).willReturn(false);
    given(mediaRepository.getExistingDigitalMedia(any())).willReturn(
        List.of(givenUnequalDigitalMediaRecord()));
    given(equalityService.setExistingEventDatesSpecimen(any(), any(), any())).willReturn(
        givenDigitalSpecimenEvent(true));
    given(equalityService.setExistingEventDatesMedia(any(), any())).willReturn(
        givenDigitalMediaEvent());
    given(entityRelationshipService.processMediaRelationshipsForSpecimen(any(), any(),
        any())).willReturn(givenEmptyMediaProcessResult());
    var pidMapSpecimen = Map.of(PHYSICAL_SPECIMEN_ID, givenPidProcessResultSpecimen(true));
    var pidMapMedia = Map.of(MEDIA_URL, givenPidProcessResultMedia());

    // When
    service.handleMessages(List.of(givenDigitalSpecimenEvent(true)));

    // Then
    then(digitalSpecimenService).should()
        .updateExistingDigitalSpecimen(
            List.of(givenUpdatedDigitalSpecimenTuple(true, givenEmptyMediaProcessResult())),
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
    var record2 = givenDigitalSpecimenRecordWithMediaEr(SECOND_HANDLE, PHYSICAL_SPECIMEN_ID_ALT,
        false);
    given(specimenRepository.getDigitalSpecimens(any())).willReturn(
        List.of(record1, record2));
    given(equalityService.specimensAreEqual(any(), any(), any())).willReturn(false);
    given(mediaRepository.getExistingDigitalMedia(Set.of(MEDIA_URL))).willReturn(
        List.of(givenUnequalDigitalMediaRecord()));
    given(equalityService.setExistingEventDatesSpecimen(any(), eq(event1), any())).willReturn(
        event1);
    given(equalityService.setExistingEventDatesSpecimen(any(), eq(event2), any())).willReturn(
        event2);
    given(equalityService.setExistingEventDatesMedia(any(), any())).willReturn(
        givenDigitalMediaEvent());

    given(entityRelationshipService.processMediaRelationshipsForSpecimen(any(), any(),
        any())).willReturn(givenEmptyMediaProcessResult());
    var pidMapSpecimen = Map.of(
        PHYSICAL_SPECIMEN_ID, givenPidProcessResultSpecimen(true),
        PHYSICAL_SPECIMEN_ID_ALT, new PidProcessResult(SECOND_HANDLE, Set.of(MEDIA_PID)));
    var pidMapMedia = Map.of(MEDIA_URL,
        new PidProcessResult(MEDIA_PID, Set.of(HANDLE, SECOND_HANDLE)));

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
    then(digitalMediaService).should()
        .createNewDigitalMedia(List.of(givenDigitalMediaEvent()), pidMapMedia);
    then(digitalSpecimenService).should()
        .createNewDigitalSpecimen(List.of(givenDigitalSpecimenEvent(true)), pidMap);
    then(digitalMediaService).should()
        .createNewDigitalMedia(List.of(givenDigitalMediaEvent()), pidMapMedia);
    then(equalityService).shouldHaveNoInteractions();
    then(digitalSpecimenService).shouldHaveNoMoreInteractions();
    then(digitalMediaService).shouldHaveNoMoreInteractions();
  }

  @Test
  void testHandleMessagesMediaNew() throws Exception {
    // Given
    given(handleComponent.postHandle(any(), eq(false))).willReturn(Map.of(MEDIA_URL, MEDIA_PID));
    var events = List.of(givenDigitalMediaEvent());
    given(digitalMediaService.createNewDigitalMedia(events,
        Map.of(MEDIA_URL, new PidProcessResult(MEDIA_PID, Set.of()))))
        .willReturn(Set.of(givenDigitalMediaRecord()));

    // When
    var result = service.handleMessagesMedia(events);

    // Then
    assertThat(result).isEqualTo(List.of(givenDigitalMediaRecord()));
    then(handleComponent).shouldHaveNoMoreInteractions();
    then(digitalMediaService).shouldHaveNoMoreInteractions();
  }

  @Test
  void testHandleMessagesMediaEqual() {
    // Given
    var events = List.of(givenDigitalMediaEvent());
    given(mediaRepository.getExistingDigitalMedia(Set.of(MEDIA_URL))).willReturn(
        List.of(givenDigitalMediaRecord()));
    given(equalityService.mediaAreEqual(givenDigitalMediaRecord(),
        givenDigitalMediaEvent().digitalMediaWrapper(), Set.of()))
        .willReturn(true);

    // When
    var result = service.handleMessagesMedia(events);

    // Then
    assertThat(result).isEmpty();
    then(handleComponent).shouldHaveNoInteractions();
    then(digitalMediaService).should().updateEqualDigitalMedia(List.of(givenDigitalMediaRecord()));
    then(digitalMediaService).shouldHaveNoMoreInteractions();
  }

  @Test
  void testHandleMessagesMediaUpdate() {
    // Given
    var events = List.of(givenDigitalMediaEvent());
    given(mediaRepository.getExistingDigitalMedia(Set.of(MEDIA_URL))).willReturn(
        List.of(givenDigitalMediaRecord()));
    given(equalityService.mediaAreEqual(givenDigitalMediaRecord(),
        givenDigitalMediaEvent().digitalMediaWrapper(), Set.of()))
        .willReturn(false);
    given(digitalMediaService.updateExistingDigitalMedia(any(),
        eq(Map.of(MEDIA_URL, new PidProcessResult(MEDIA_PID, Set.of()))))).willReturn(
        Set.of(givenDigitalMediaRecord()));

    // When
    var result = service.handleMessagesMedia(events);

    // Then
    assertThat(result).isEqualTo(List.of(givenDigitalMediaRecord()));
    then(handleComponent).shouldHaveNoInteractions();
    then(digitalMediaService).shouldHaveNoMoreInteractions();
  }

  @Test
  void testHandleMessagesMediaEqualDuplicates() throws Exception {
    // Given
    var events = List.of(givenDigitalMediaEvent(), givenDigitalMediaEvent());
    given(mediaRepository.getExistingDigitalMedia(Set.of(MEDIA_URL))).willReturn(
        List.of(givenDigitalMediaRecord()));
    given(equalityService.mediaAreEqual(givenDigitalMediaRecord(),
        givenDigitalMediaEvent().digitalMediaWrapper(), Set.of()))
        .willReturn(true);

    // When
    var result = service.handleMessagesMedia(events);

    // Then
    assertThat(result).isEmpty();
    then(handleComponent).shouldHaveNoInteractions();
    then(digitalMediaService).should().updateEqualDigitalMedia(List.of(givenDigitalMediaRecord()));
    then(digitalMediaService).shouldHaveNoMoreInteractions();
    then(publisherService).should(times(1)).republishMediaEvent(givenDigitalMediaEvent());
  }


  @Test
  void testNewSpecimenTooManyNewMedia() {
    // Given
    var mediaEvents = new ArrayList<DigitalMediaEvent>();
    IntStream.rangeClosed(0, 10000)
        .forEach(i -> mediaEvents.add(givenDigitalMediaEvent(UUID.randomUUID().toString())));
    var specimenEvents = List.of(new DigitalSpecimenEvent(
        List.of(),
        givenDigitalSpecimenWrapper(),
        mediaEvents
    ));

    // When / Then
    assertThrows(TooManyObjectsException.class, () -> service.handleMessages(specimenEvents));
  }

  @Test
  void testNewSpecimenNewMediaBatchedPid() throws Exception {
    given(specimenRepository.getDigitalSpecimens(any())).willReturn(
        List.of());
    given(mediaRepository.getExistingDigitalMedia(any())).willReturn(List.of());
    given(handleComponent.postHandle(any(), eq(true))).willReturn(
        TestUtils.givenHandleResponseSpecimen());
    given(fdoRecordService.buildPostHandleRequest(any())).willReturn(List.of(givenHandleRequest()));
    given(fdoRecordService.buildPostRequestMedia(any())).willReturn(List.of(givenHandleRequest()));
    var mediaEvents = new ArrayList<DigitalMediaEvent>();
    IntStream.rangeClosed(0, 2002)
        .forEach(i -> mediaEvents.add(givenDigitalMediaEvent(UUID.randomUUID().toString())));
    var specimenEvents = List.of(new DigitalSpecimenEvent(
        List.of(),
        givenDigitalSpecimenWrapper(false, true),
        mediaEvents
    ));

    // When
    service.handleMessages(specimenEvents);

    // Then
    then(handleComponent).should(times(4)).postHandle(any(), anyBoolean());
  }

}
