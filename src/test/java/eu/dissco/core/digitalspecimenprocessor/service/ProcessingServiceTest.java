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
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenEventWithMediaEr;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenRecord;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenRecordWithMediaEr;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenWithEntityRelationship;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenWrapper;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenWrapperNoOriginalData;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenEmptyMediaProcessResult;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenEmptyMediaProcessResultMap;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenHandleComponentResponse;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenHandleRequest;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenJsonPatch;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenMediaPidResponse;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenMediaProcessResultMapNew;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenMediaProcessResultUnchanged;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenUnequalDigitalSpecimenRecord;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.eq;
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
import com.fasterxml.jackson.databind.JsonNode;
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
import java.util.stream.Stream;
import org.jooq.exception.DataAccessException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
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

  private MockedStatic<Instant> mockedInstant;
  private MockedStatic<Clock> mockedClock;
  private ProcessingService service;

  private static Stream<Arguments> provideUnequalDigitalSpecimen() {
    return Stream.of(
        Arguments.of(new DigitalSpecimenRecord(HANDLE, MIDS_LEVEL, VERSION, CREATED,
            new DigitalSpecimenWrapper(PHYSICAL_SPECIMEN_ID, ANOTHER_SPECIMEN_NAME,
                new DigitalSpecimen().withOdsTopicDiscipline(OdsTopicDiscipline.ECOLOGY), null))),
        Arguments.of(new DigitalSpecimenRecord(HANDLE, MIDS_LEVEL, VERSION, CREATED,
            new DigitalSpecimenWrapper(PHYSICAL_SPECIMEN_ID, ANOTHER_SPECIMEN_NAME,
                new DigitalSpecimen(), null)))
    );
  }

  @BeforeEach
  void setup() {
    service = new ProcessingService(repository, fdoRecordService, elasticRepository, kafkaService,
        midsService, handleComponent, applicationProperties, annotationPublisherService, MAPPER,
        digitalMediaService);
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

  @ParameterizedTest
  @MethodSource("provideUnequalDigitalSpecimen")
  void testUnequalSpecimen(DigitalSpecimenRecord currentSpecimenRecord) throws Exception {
    // Given
    var expected = List.of(givenDigitalSpecimenRecord(2, true));
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
    given(repository.getDigitalSpecimens(List.of(PHYSICAL_SPECIMEN_ID))).willReturn(
        List.of(currentSpecimenRecord));
    given(bulkResponse.errors()).willReturn(false);
    given(elasticRepository.indexDigitalSpecimen(expected)).willReturn(bulkResponse);
    given(midsService.calculateMids(givenDigitalSpecimenWrapper(true))).willReturn(1);
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);
    given(applicationProperties.getPid()).willReturn(APP_HANDLE);
    given(applicationProperties.getName()).willReturn(APP_NAME);
    given(digitalMediaService.getExistingDigitalMedia(any(), anyList())).willReturn(
        Map.of(HANDLE, new DigitalMediaProcessResult(List.of(), List.of(), currentMediaEvent)));
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
    var expected = List.of(givenDigitalSpecimenRecord(2, true));
    given(repository.getDigitalSpecimens(List.of(PHYSICAL_SPECIMEN_ID))).willReturn(
        List.of(currentSpecimenRecord));
    given(bulkResponse.errors()).willReturn(false);
    given(elasticRepository.indexDigitalSpecimen(expected)).willReturn(bulkResponse);
    given(midsService.calculateMids(givenDigitalSpecimenWrapper(true))).willReturn(1);
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);
    given(applicationProperties.getPid()).willReturn(APP_HANDLE);
    given(applicationProperties.getName()).willReturn(APP_NAME);
    given(digitalMediaService.getExistingDigitalMedia(any(), anyList())).willReturn(
        Map.of(HANDLE, new DigitalMediaProcessResult(List.of(), mediaEr, List.of())));

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
  void testOriginalDataChanged() throws Exception {
    // Given
    var currentSpecimenRecord = givenDigitalSpecimenRecord();
    given(repository.getDigitalSpecimens(List.of(PHYSICAL_SPECIMEN_ID))).willReturn(
        List.of(currentSpecimenRecord));
    var event = new DigitalSpecimenEvent(
        List.of(MAS),
        givenDigitalSpecimenWrapperNoOriginalData(),
        List.of());
    given(digitalMediaService.getExistingDigitalMedia(any(), anyList())).willReturn(
        Map.of(HANDLE, givenEmptyMediaProcessResult()));

    // When
    var result = service.handleMessages(List.of(event));

    // Then
    verifyNoInteractions(handleComponent);
    verifyNoInteractions(fdoRecordService);
    verifyNoInteractions(annotationPublisherService);
    then(repository).should().updateLastChecked(List.of(HANDLE));
    assertThat(result).isEmpty();
  }

  @Test
  void testHandleRecordDoesNotNeedUpdate() throws Exception {
    // Given
    var expected = List.of(givenDigitalSpecimenRecord(2, true));
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
    given(fdoRecordService.buildPostHandleRequest(anyList())).willReturn(
        List.of(givenHandleRequest()));
    given(handleComponent.postHandle(anyList())).willThrow(PidException.class);

    // When
    var result = service.handleMessages(List.of(givenDigitalSpecimenEvent()));

    // Then
    then(handleComponent).should().rollbackFromPhysId(List.of(PHYSICAL_SPECIMEN_ID));
    then(kafkaService).should().deadLetterEvent(givenDigitalSpecimenEvent());
    assertThat(result).isEmpty();
  }

  @Test
  void testNewSpecimenPidAndKafkaFailed() throws Exception {
    // Given
    given(repository.getDigitalSpecimens(List.of(PHYSICAL_SPECIMEN_ID))).willReturn(List.of());
    given(handleComponent.postHandle(anyList())).willThrow(PidException.class);
    given(fdoRecordService.buildPostHandleRequest(anyList())).willReturn(
        List.of(givenHandleRequest()));
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
    verify(handleComponent, times(1)).postHandle(anyList()
    );
    then(repository).should().createDigitalSpecimenRecord(Set.of(givenDigitalSpecimenRecord()));
    then(kafkaService).should().publishCreateEvent(givenDigitalSpecimenRecord());
    then(kafkaService).should().publishAnnotationRequestEvent(MAS, givenDigitalSpecimenRecord());
    then(kafkaService).should().republishEvent(duplicateSpecimen);
    assertThat(result).isEqualTo(List.of(givenDigitalSpecimenRecord()));
  }

  @Test
  void testNewSpecimenRollbackHandleCreationFailed() throws Exception {
    // Given
    given(repository.getDigitalSpecimens(List.of(PHYSICAL_SPECIMEN_ID))).willReturn(List.of());
    given(midsService.calculateMids(givenDigitalSpecimenWrapper())).willReturn(1);
    given(fdoRecordService.buildPostHandleRequest(anyList())).willReturn(
        List.of(givenHandleRequest()));
    given(
        elasticRepository.indexDigitalSpecimen(Set.of(givenDigitalSpecimenRecord()))).willThrow(
        IOException.class);
    doThrow(PidException.class).when(handleComponent).rollbackHandleCreation(any());
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
    given(fdoRecordService.buildPostHandleRequest(anyList())).willReturn(
        List.of(givenHandleRequest()));
    given(midsService.calculateMids(givenDigitalSpecimenWrapper())).willReturn(1);
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
    var secondSpecimen = givenDigitalSpecimenRecordWithMediaEr(SECOND_HANDLE, secondPhysicalId,
        false);
    var thirdSpecimen = givenDigitalSpecimenRecordWithMediaEr(THIRD_HANDLE, thirdPhysicalId, false);
    given(repository.getDigitalSpecimens(anyList())).willReturn(List.of());
    given(midsService.calculateMids(any(DigitalSpecimenWrapper.class))).willReturn(1);
    givenBulkResponse();
    given(fdoRecordService.buildPostHandleRequest(anyList())).willReturn(
        List.of(givenHandleRequest()));
    given(fdoRecordService.buildPostRequestMedia(any(), any())).willReturn(
        List.of(MAPPER.createObjectNode()));
    given(elasticRepository.indexDigitalSpecimen(anySet())).willReturn(bulkResponse);
    given(applicationProperties.getPid()).willReturn(APP_HANDLE);
    given(applicationProperties.getName()).willReturn(APP_NAME);
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

    // When
    var result = service.handleMessages(
        List.of(givenDigitalSpecimenEvent(), givenDigitalSpecimenEvent(secondPhysicalId),
            givenDigitalSpecimenEvent(thirdPhysicalId)));

    // Then
    then(repository).should().createDigitalSpecimenRecord(anySet());
    then(repository).should().rollbackSpecimen(secondSpecimen.id());
    then(fdoRecordService).should().buildRollbackCreationRequest(List.of(secondSpecimen));
    then(handleComponent).should().rollbackHandleCreation(any());
    then(handleComponent).should().postHandle(any());
    then(kafkaService).should()
        .deadLetterEvent(givenDigitalSpecimenEventWithMediaEr(secondPhysicalId));
    then(kafkaService).should()
        .publishDigitalMediaObject(any(DigitalMediaEvent.class));
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
    then(repository).should().createDigitalSpecimenRecord(anySet());
    then(elasticRepository).should().rollbackSpecimen(givenDigitalSpecimenRecord());
    then(repository).should().rollbackSpecimen(givenDigitalSpecimenRecord().id());
    then(handleComponent).should().rollbackHandleCreation(any());
    then(kafkaService).should().deadLetterEvent(givenDigitalSpecimenEvent());
    then(kafkaService).shouldHaveNoMoreInteractions();
    then(annotationPublisherService).should().publishAnnotationNewSpecimen(Set.of());
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
    doThrow(PidException.class).when(handleComponent).updateHandle(any());

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
    doThrow(PidException.class).when(handleComponent).updateHandle(any());
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
    var events = List.of(firstEvent, secondEvent, thirdEvent);
    given(repository.getDigitalSpecimens(anyList()))
        .willReturn(List.of(givenDifferentUnequalSpecimen(THIRD_HANDLE, "A third Specimen"),
            givenDifferentUnequalSpecimen(SECOND_HANDLE, "Another Specimen"),
            givenUnequalDigitalSpecimenRecord()
        ));
    givenBulkResponse();
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);
    given(elasticRepository.indexDigitalSpecimen(anyList())).willReturn(bulkResponse);
    given(midsService.calculateMids(firstEvent.digitalSpecimenWrapper())).willReturn(1);
    given(digitalMediaService.getExistingDigitalMedia(any(), anyList()))
        .willReturn(givenMediaProcessResultMapNew(Map.of(
            THIRD_HANDLE, thirdEvent,
            SECOND_HANDLE, secondEvent,
            HANDLE, firstEvent
        )));

    // When
    var result = service.handleMessages(events);

    // Then
    then(handleComponent).should().updateHandle(anyList());
    then(handleComponent).should().rollbackHandleUpdate(any());
    then(repository).should(times(2)).createDigitalSpecimenRecord(anyList());
    then(kafkaService).should().deadLetterEvent(secondEvent);
    then(kafkaService).should(times(2))
        .publishDigitalMediaObject(any(DigitalMediaEvent.class));
    assertThat(result).hasSize(2);
  }

  @Test
  void testUpdateSpecimenPartialElasticAndRollbackFailed() throws Exception {
    // Given
    var firstEvent = givenDigitalSpecimenEvent(true);
    var secondEvent = givenDigitalSpecimenEvent("Another Specimen");
    var thirdEvent = givenDigitalSpecimenEvent("A third Specimen");
    var events = List.of(firstEvent, secondEvent, thirdEvent);
    given(repository.getDigitalSpecimens(
        List.of(PHYSICAL_SPECIMEN_ID, "A third Specimen", "Another Specimen")))
        .willReturn(List.of(
            givenUnequalDigitalSpecimenRecord(),
            givenDifferentUnequalSpecimen(THIRD_HANDLE, "A third Specimen"),
            givenDifferentUnequalSpecimen(SECOND_HANDLE, "Another Specimen")));
    givenBulkResponse();
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
    doThrow(PidException.class).when(handleComponent).rollbackHandleUpdate(any());

    // When
    var result = service.handleMessages(events);

    // Then
    then(fdoRecordService).should().buildRollbackUpdateRequest(
        List.of(givenDifferentUnequalSpecimen(SECOND_HANDLE, "Another Specimen")));
    then(handleComponent).should().updateHandle(anyList());
    then(handleComponent).should().rollbackHandleUpdate(any());
    then(repository).should(times(2)).createDigitalSpecimenRecord(anyList());
    then(kafkaService).should().deadLetterEvent(secondEvent);
    then(kafkaService).should(times(2))
        .publishDigitalMediaObject(any(DigitalMediaEvent.class));
    assertThat(result).hasSize(2);
  }

  @Test
  void testUpdateSpecimenKafkaFailed() throws Exception {
    given(repository.getDigitalSpecimens(List.of(PHYSICAL_SPECIMEN_ID))).willReturn(
        List.of(givenUnequalDigitalSpecimenRecord()));
    given(midsService.calculateMids(givenDigitalSpecimenWrapper())).willReturn(1);
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
    then(fdoRecordService).should()
        .buildUpdateHandleRequest(anyList());
    then(fdoRecordService).should()
        .buildRollbackUpdateRequest(List.of(unequalCurrentDigitalSpecimen));
    then(handleComponent).should().updateHandle(any());
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
    then(kafkaService).should().republishEvent(givenDigitalSpecimenEvent());
    then(kafkaService).shouldHaveNoMoreInteractions();
    then(fdoRecordService).shouldHaveNoInteractions();
  }

  @Test
  void testCreateSpecimenThrowsDataAccessException() throws Exception {
    // Given
    var firstEvent = givenDigitalSpecimenEvent(true);
    var secondEvent = givenDigitalSpecimenEvent("Another Specimen");
    var thirdEvent = givenDigitalSpecimenEvent("A third Specimen");
    var events = List.of(firstEvent, secondEvent, thirdEvent);
    var unequalOriginalSpecimens = List.of(
        givenDifferentUnequalSpecimen(THIRD_HANDLE, "A third Specimen"),
        givenDifferentUnequalSpecimen(SECOND_HANDLE, "Another Specimen"),
        givenUnequalDigitalSpecimenRecord()
    );
    given(repository.getDigitalSpecimens(anyList()))
        .willReturn(unequalOriginalSpecimens);
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);
    given(midsService.calculateMids(firstEvent.digitalSpecimenWrapper())).willReturn(1);
    doThrow(DataAccessException.class).when(repository).createDigitalSpecimenRecord(anyList());
    given(digitalMediaService.getExistingDigitalMedia(any(), anyList()))
        .willReturn(givenMediaProcessResultMapNew(Map.of(
            THIRD_HANDLE, thirdEvent,
            SECOND_HANDLE, secondEvent,
            HANDLE, firstEvent)));

    // When
    var result = service.handleMessages(events);

    // Then
    then(fdoRecordService).should().buildRollbackUpdateRequest(anyList());
    then(handleComponent).should().rollbackHandleUpdate(any());
    then(kafkaService).should(times(3)).deadLetterEvent(any());
    assertThat(result).isEmpty();
  }

  @Test
  void testUpdateSpecimenThrowsDataAccessException() throws Exception {
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
    then(fdoRecordService).should().buildRollbackCreationRequest(any());
    then(handleComponent).should().rollbackHandleCreation(any());
    then(kafkaService).should().deadLetterEvent(newSpecimenEvent);
    assertThat(result).isEmpty();
  }

}
