package eu.dissco.core.digitalspecimenprocessor.service;

import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.CREATED;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MAPPER;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MEDIA_PID;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MEDIA_URL;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalMediaEvent;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalMediaRecord;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenPidProcessResultMedia;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.mockStatic;

import co.elastic.clients.elasticsearch.core.BulkResponse;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dissco.core.digitalspecimenprocessor.repository.DigitalMediaRepository;
import eu.dissco.core.digitalspecimenprocessor.repository.ElasticSearchRepository;
import eu.dissco.core.digitalspecimenprocessor.web.HandleComponent;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DigitalMediaServiceTest {

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

  @BeforeEach
  void setup() {
    mediaService = new DigitalMediaService(repository, fdoRecordService, handleComponent, MAPPER,
        rollbackService, elasticRepository, annotationPublisherService, publisherService);
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
  void testUpdateEqualMedia(){
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
  }
  
  
  
  /*
  @BeforeEach
  void setup() {
    mediaService = new DigitalMediaService(mediaRepository);
  }

  @Test
  void testGetExistingDigitalMediaNew() {
    //
    var firstRecord = givenDigitalSpecimenRecord();
    var secondRecord = givenDifferentUnequalSpecimen(SECOND_HANDLE, PHYSICAL_SPECIMEN_ID_ALT);
    var currentSpecimens = Map.of(PHYSICAL_SPECIMEN_ID, firstRecord, PHYSICAL_SPECIMEN_ID_ALT,
        secondRecord);
    var firstMediaEventA = givenDigitalMediaEvent();
    var firstMediaEventB = givenDigitalMediaEvent(PHYSICAL_SPECIMEN_ID, MEDIA_URL_ALT);
    var secondMediaEvent = givenDigitalMediaEvent(PHYSICAL_SPECIMEN_ID_ALT);
    var expected = Map.of(
        HANDLE, new MediaRelationshipProcessResult(List.of(),
            List.of(firstMediaEventA, firstMediaEventB)),
        SECOND_HANDLE,
        new MediaRelationshipProcessResult(List.of(), List.of(secondMediaEvent))
    );
    given(mediaRepository.getDigitalMediaUrisFromIdKey(List.of())).willReturn(Map.of());

    // When
    var result = mediaService.getExistingDigitalMedia(currentSpecimens,
        List.of(firstMediaEventA, firstMediaEventB, secondMediaEvent));

    // Then
    assertThat(expected).isEqualTo(result);
  }

  @Test
  void testGetExistingMediaRelationshipsUnchanged() {
    // Given
    var firstRecord = givenDigitalSpecimenRecordWithMediaEr();
    var secondRecord = givenDigitalSpecimenRecordWithMediaEr(SECOND_HANDLE,
        PHYSICAL_SPECIMEN_ID_ALT, true, 1, MEDIA_PID_ALT);
    var secondRecordMediaER = secondRecord.digitalSpecimenWrapper().attributes()
        .getOdsHasEntityRelationships().stream()
        .filter(er -> er.getDwcRelationshipOfResource().equals(HAS_MEDIA.getName()))
        .toList();
    var currentSpecimens = Map.of(PHYSICAL_SPECIMEN_ID, firstRecord, PHYSICAL_SPECIMEN_ID_ALT,
        secondRecord);
    var mediaEvents = List.of(givenDigitalMediaEvent(),
        givenDigitalMediaEvent(PHYSICAL_SPECIMEN_ID_ALT, MEDIA_URL_ALT));
    var expected = Map.of(
        HANDLE, new MediaRelationshipProcessResult(List.of(), List.of()),
        SECOND_HANDLE, new MediaRelationshipProcessResult(List.of(), List.of()));
    given(mediaRepository.getDigitalMediaUrisFromIdKey(anyList())).willReturn(Map.of(
        new DigitalMediaKey(HANDLE, MEDIA_URL), MEDIA_PID,
        new DigitalMediaKey(SECOND_HANDLE, MEDIA_URL_ALT), MEDIA_PID_ALT));

    // When
    var result = mediaService.getExistingDigitalMedia(currentSpecimens, mediaEvents);

    // Then
    assertThat(expected).isEqualTo(result);
    then(mediaRepository).should().getDigitalMediaUrisFromIdKey(mediaPidsCaptor.capture());
    assertThat(mediaPidsCaptor.getValue()).containsExactlyInAnyOrder(MEDIA_PID, MEDIA_PID_ALT);
  }

  @Test
  void testGetExistingMediaRelationshipsException() {
    // Given
    var firstRecord = givenDigitalSpecimenRecord();
    var secondRecord = givenDifferentUnequalSpecimen(SECOND_HANDLE,
        PHYSICAL_SPECIMEN_ID_ALT);
    var currentSpecimens = Map.of(PHYSICAL_SPECIMEN_ID, firstRecord, PHYSICAL_SPECIMEN_ID_ALT,
        secondRecord);
    var mediaEvents = List.of(givenDigitalMediaEvent(),
        givenDigitalMediaEvent(PHYSICAL_SPECIMEN_ID_ALT, MEDIA_URL_ALT));
    given(mediaRepository.getDigitalMediaUrisFromIdKey(anyList())).willReturn(Map.of(
        new DigitalMediaKey(HANDLE, MEDIA_URL), MEDIA_PID,
        new DigitalMediaKey(SECOND_HANDLE, MEDIA_URL_ALT), MEDIA_PID_ALT));

    // When / Then
    assertThrows(IllegalStateException.class,
        () -> mediaService.getExistingDigitalMedia(currentSpecimens, mediaEvents));
  }

  @Test
  void testGetExistingMediaRelationshipsTombstone() {
    // Given
    var firstRecord = givenDigitalSpecimenRecordWithMediaEr();
    var secondRecord = givenDigitalSpecimenRecordWithMediaEr(SECOND_HANDLE,
        PHYSICAL_SPECIMEN_ID_ALT, false, 1, MEDIA_PID_ALT);
    var currentSpecimens = Map.of(PHYSICAL_SPECIMEN_ID, firstRecord, PHYSICAL_SPECIMEN_ID_ALT,
        secondRecord);
    var expected = Map.of(
        HANDLE, new MediaRelationshipProcessResult(
            firstRecord.digitalSpecimenWrapper().attributes().getOdsHasEntityRelationships(),
            List.of()),
        SECOND_HANDLE, new MediaRelationshipProcessResult(
            secondRecord.digitalSpecimenWrapper().attributes().getOdsHasEntityRelationships(),
            List.of()));
    given(mediaRepository.getDigitalMediaUrisFromIdKey(anyList())).willReturn(Map.of(
        new DigitalMediaKey(HANDLE, MEDIA_URL), MEDIA_PID,
        new DigitalMediaKey(SECOND_HANDLE, MEDIA_URL_ALT), MEDIA_PID_ALT));

    // When
    var result = mediaService.getExistingDigitalMedia(currentSpecimens, Collections.emptyList());

    // Then
    assertThat(expected).isEqualTo(result);
    then(mediaRepository).should().getDigitalMediaUrisFromIdKey(mediaPidsCaptor.capture());
    assertThat(mediaPidsCaptor.getValue()).containsExactlyInAnyOrder(MEDIA_PID, MEDIA_PID_ALT);
  }

  @Test
  void testGetExistingMediaRelationshipsNoMedia() {
    // Given
    var currentSpecimens = Map.of(PHYSICAL_SPECIMEN_ID, givenDigitalSpecimenRecord());
    var expected = Map.of(HANDLE, new MediaRelationshipProcessResult(List.of(), List.of()));
    given(mediaRepository.getDigitalMediaUrisFromIdKey(anyList())).willReturn(Map.of());

    // When
    var result = mediaService.getExistingDigitalMedia(currentSpecimens, List.of());

    // Then
    assertThat(expected).isEqualTo(result);
  }

  @Test
  void testRemoveSpecimenRelationshipsFromMedia() {
    // Given
    var currentDigitalSpecimenRecord = givenDigitalSpecimenRecordWithMediaEr(HANDLE,
        PHYSICAL_SPECIMEN_ID, true);
    var tombstonedEr = currentDigitalSpecimenRecord.digitalSpecimenWrapper().attributes()
        .getOdsHasEntityRelationships().stream().filter(
            entityRelationship -> entityRelationship.getDwcRelationshipOfResource()
                .equals(HAS_MEDIA.getName()))
        .toList();

    var updatedSpecimenRecord = new UpdatedDigitalSpecimenRecord(
        givenDigitalSpecimenRecord(),
        List.of(),
        currentDigitalSpecimenRecord,
        MAPPER.createObjectNode(),
        List.of(),
        new MediaRelationshipProcessResult(tombstonedEr, List.of())
    );

    // When
    mediaService.removeSpecimenRelationshipsFromMedia(Set.of(updatedSpecimenRecord));

    // Then
    then(mediaRepository).should().removeSpecimenRelationshipsFromMedia(List.of(MEDIA_PID));
  }*/

}
