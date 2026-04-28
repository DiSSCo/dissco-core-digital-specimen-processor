package eu.dissco.core.digitalspecimenprocessor.service.preprocessing;

import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.ANOTHER_SPECIMEN_NAME;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.CREATED;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.HANDLE;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MAPPER;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MAS;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MEDIA_PID;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MEDIA_PID_ALT;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MEDIA_URL;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MEDIA_URL_ALT;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.ORGANISATION_ID;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.ORIGINAL_DATA;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.PHYSICAL_SPECIMEN_ID;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.PHYSICAL_SPECIMEN_ID_ALT;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.SECOND_HANDLE;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.SPECIMEN_NAME;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalMediaEvent;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalMediaRecord;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenEvent;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenRecord;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenRecordWithMediaEr;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenWrapper;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenEmptyMediaProcessResult;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenEntityRelationship;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenHandleResponse;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenHandleResponseSpecimen;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenPidProcessResultMedia;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenPidProcessResultSpecimen;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenPidResponseMedia;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenUnequalDigitalMediaRecord;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenUnequalDigitalSpecimenRecord;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenUpdatedDigitalMediaTuple;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenUpdatedDigitalSpecimenTuple;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;

import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaEvent;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenEvent;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.SpecimenProcessResult;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.UpdatedDigitalSpecimenTuple;
import eu.dissco.core.digitalspecimenprocessor.exception.AnnotationProcessingException;
import eu.dissco.core.digitalspecimenprocessor.exception.DisscoRepositoryException;
import eu.dissco.core.digitalspecimenprocessor.exception.PidException;
import eu.dissco.core.digitalspecimenprocessor.exception.TooManyObjectsException;
import eu.dissco.core.digitalspecimenprocessor.property.ApplicationProperties;
import eu.dissco.core.digitalspecimenprocessor.repository.DigitalMediaRepository;
import eu.dissco.core.digitalspecimenprocessor.repository.DigitalSpecimenRepository;
import eu.dissco.core.digitalspecimenprocessor.schema.EntityRelationship;
import eu.dissco.core.digitalspecimenprocessor.service.AnnotationService;
import eu.dissco.core.digitalspecimenprocessor.service.DigitalMediaService;
import eu.dissco.core.digitalspecimenprocessor.service.DigitalSpecimenService;
import eu.dissco.core.digitalspecimenprocessor.service.EntityRelationshipService;
import eu.dissco.core.digitalspecimenprocessor.service.EqualityService;
import eu.dissco.core.digitalspecimenprocessor.service.FdoRecordService;
import eu.dissco.core.digitalspecimenprocessor.service.MasSchedulerService;
import eu.dissco.core.digitalspecimenprocessor.service.RabbitMqPublisherService;
import eu.dissco.core.digitalspecimenprocessor.utils.TestUtils;
import eu.dissco.core.digitalspecimenprocessor.web.PidComponent;
import java.net.URI;
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
class SpecimenPreprocessingServiceTest {

	@Mock
	private DigitalSpecimenRepository specimenRepository;

	@Mock
	private DigitalMediaRepository mediaRepository;

	@Mock
	private FdoRecordService fdoRecordService;

	@Mock
	private RabbitMqPublisherService publisherService;

	@Mock
	private PidComponent pidComponent;

	@Mock
	private DigitalMediaService digitalMediaService;

	@Mock
	private EntityRelationshipService entityRelationshipService;

	@Mock
	private EqualityService equalityService;

	@Mock
	private DigitalSpecimenService digitalSpecimenService;

	@Mock
	private MasSchedulerService masSchedulerService;

	@Mock
	AnnotationService annotationService;

	private MockedStatic<Instant> mockedInstant;

	private SpecimenPreprocessingService service;

	@BeforeEach
	void setup() {
		service = new SpecimenPreprocessingService(MAPPER, mediaRepository, digitalMediaService, publisherService,
				entityRelationshipService, equalityService, new ApplicationProperties(), fdoRecordService, pidComponent,
				masSchedulerService, specimenRepository, annotationService, digitalSpecimenService);
		Clock clock = Clock.fixed(CREATED, ZoneOffset.UTC);
		Instant instant = Instant.now(clock);
		mockedInstant = mockStatic(Instant.class, CALLS_REAL_METHODS);
		mockedInstant.when(Instant::now).thenReturn(instant);
	}

	@AfterEach
	void destroy() {
		mockedInstant.close();
	}

	@Test
	void testEqualSpecimenNoMedia() throws Exception {
		// Given
		given(specimenRepository.getDigitalSpecimens(List.of(PHYSICAL_SPECIMEN_ID)))
			.willReturn(List.of(givenDigitalSpecimenRecord()));
		given(entityRelationshipService.processMediaRelationshipsForSpecimen(anyMap(), any(), anyMap()))
			.willReturn(givenEmptyMediaProcessResult());
		given(equalityService.specimensAreEqual(any(), any(), any())).willReturn(true);
		given(annotationService.applyAcceptedAnnotations(any(), any(), any())).willReturn(givenDigitalSpecimenEvent());

		// When
		var result = service.handleMessages(List.of(givenDigitalSpecimenEvent()));

		// Then
		assertThat(result).isEqualTo(new SpecimenProcessResult(
				Map.of(givenDigitalSpecimenRecord(), givenDigitalSpecimenEvent()), List.of(), List.of()));
		then(digitalSpecimenService).should()
			.updateEqualSpecimen(Map.of(givenDigitalSpecimenRecord(), givenDigitalSpecimenEvent()));
		then(digitalSpecimenService).shouldHaveNoMoreInteractions();
		then(pidComponent).shouldHaveNoInteractions();
		then(digitalMediaService).shouldHaveNoInteractions();
	}

	@Test
	void testAnnotationApplicationFailed() throws Exception {
		// Given
		given(specimenRepository.getDigitalSpecimens(List.of(PHYSICAL_SPECIMEN_ID)))
			.willReturn(List.of(givenDigitalSpecimenRecord()));
		given(entityRelationshipService.processMediaRelationshipsForSpecimen(anyMap(), any(), anyMap()))
			.willReturn(givenEmptyMediaProcessResult());
		given(annotationService.applyAcceptedAnnotations(any(), any(), any()))
			.willThrow(new AnnotationProcessingException());

		// When
		var result = service.handleMessages(List.of(givenDigitalSpecimenEvent()));

		// Then
		assertThat(result).isEqualTo(new SpecimenProcessResult(Map.of(), List.of(), List.of()));
		then(publisherService).should().deadLetterEventSpecimen(givenDigitalSpecimenEvent());
		then(digitalSpecimenService).shouldHaveNoInteractions();
		then(pidComponent).shouldHaveNoInteractions();
		then(digitalMediaService).shouldHaveNoInteractions();
	}

	@Test
	void testEqualSpecimenNoMediaDuplicate() throws Exception {
		// Given
		given(specimenRepository.getDigitalSpecimens(List.of(PHYSICAL_SPECIMEN_ID)))
			.willReturn(List.of(givenDigitalSpecimenRecord()));
		given(entityRelationshipService.processMediaRelationshipsForSpecimen(anyMap(), any(), anyMap()))
			.willReturn(givenEmptyMediaProcessResult());
		given(annotationService.applyAcceptedAnnotations(any(), any(), any())).willReturn(givenDigitalSpecimenEvent());
		given(annotationService.applyAcceptedAnnotations(any(), any(), any())).willReturn(givenDigitalSpecimenEvent());
		given(equalityService.specimensAreEqual(any(), any(), any())).willReturn(true);
		given(annotationService.applyAcceptedAnnotations(any(), any(), any())).willReturn(givenDigitalSpecimenEvent());

		// When
		var result = service.handleMessages(List.of(givenDigitalSpecimenEvent(), givenDigitalSpecimenEvent()));

		// Then
		assertThat(result).isEqualTo(new SpecimenProcessResult(
				Map.of(givenDigitalSpecimenRecord(), givenDigitalSpecimenEvent()), List.of(), List.of()));
		then(digitalSpecimenService).should()
			.updateEqualSpecimen(Map.of(givenDigitalSpecimenRecord(), givenDigitalSpecimenEvent()));
		then(digitalSpecimenService).shouldHaveNoMoreInteractions();
		then(pidComponent).shouldHaveNoInteractions();
		then(digitalMediaService).shouldHaveNoInteractions();
		then(publisherService).should().republishSpecimenEvent(givenDigitalSpecimenEvent());
	}

	@Test
	void testNewSpecimenNoMedia() throws Exception {
		given(specimenRepository.getDigitalSpecimens(List.of(PHYSICAL_SPECIMEN_ID))).willReturn(List.of());
		given(pidComponent.postPid(any(), eq(true))).willReturn(givenHandleResponseSpecimen());
		given(fdoRecordService.buildPostPidRequest(any())).willReturn(List.of(TestUtils.givenPidRequest()));
		var pidMap = Map.of(PHYSICAL_SPECIMEN_ID, givenPidProcessResultSpecimen(false));
		given(digitalSpecimenService.createNewDigitalSpecimen(any(), any()))
			.willReturn(Set.of(givenDigitalSpecimenRecord()));

		// When
		service.handleMessages(List.of(givenDigitalSpecimenEvent()));

		// Then
		then(digitalSpecimenService).should().createNewDigitalSpecimen(List.of(givenDigitalSpecimenEvent()), pidMap);
		then(equalityService).shouldHaveNoInteractions();
		then(digitalSpecimenService).shouldHaveNoMoreInteractions();
		then(pidComponent).shouldHaveNoMoreInteractions();
		then(digitalMediaService).shouldHaveNoInteractions();
		then(masSchedulerService).should().scheduleMasForSpecimen(any());
		then(masSchedulerService).should().scheduleMasForMedia(any());
	}

	@Test
	void testNewSpecimenNoMediaPidFails() throws Exception {
		given(specimenRepository.getDigitalSpecimens(List.of(PHYSICAL_SPECIMEN_ID))).willReturn(List.of());
		given(fdoRecordService.buildPostPidRequest(any())).willReturn(List.of(TestUtils.givenPidRequest()));
		given(pidComponent.postPid(any(), eq(true))).willThrow(PidException.class);

		// When
		var result = service.handleMessages(List.of(givenDigitalSpecimenEvent()));

		// Then
		assertThat(result).isEqualTo(new SpecimenProcessResult(Map.of(), List.of(), List.of()));
		then(digitalSpecimenService).shouldHaveNoInteractions();
		then(equalityService).shouldHaveNoInteractions();
		then(pidComponent).shouldHaveNoMoreInteractions();
		then(digitalMediaService).shouldHaveNoInteractions();
	}

	@Test
	void testChangedSpecimenNoMedia() throws Exception {
		given(specimenRepository.getDigitalSpecimens(List.of(PHYSICAL_SPECIMEN_ID)))
			.willReturn(List.of(givenUnequalDigitalSpecimenRecord()));
		given(equalityService.specimensAreEqual(any(), any(), any())).willReturn(false);
		given(equalityService.setExistingEventDatesSpecimen(any(), any(), any()))
			.willReturn(givenDigitalSpecimenEvent());
		given(entityRelationshipService.processMediaRelationshipsForSpecimen(any(), any(), any()))
			.willReturn(givenEmptyMediaProcessResult());
		given(annotationService.applyAcceptedAnnotations(any(), any(), any())).willReturn(givenDigitalSpecimenEvent());
		var pidMap = Map.of(PHYSICAL_SPECIMEN_ID, givenPidProcessResultSpecimen(false));

		// When
		var result = service.handleMessages(List.of(givenDigitalSpecimenEvent()));

		// Then
		assertThat(result).isEqualTo(new SpecimenProcessResult(Map.of(), List.of(), List.of()));
		then(digitalSpecimenService).should()
			.updateExistingDigitalSpecimen(
					List.of(givenUpdatedDigitalSpecimenTuple(false, givenEmptyMediaProcessResult())), pidMap);
		then(equalityService).shouldHaveNoMoreInteractions();
		then(digitalSpecimenService).shouldHaveNoMoreInteractions();
		then(pidComponent).shouldHaveNoInteractions();
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
		assertThat(result).isEqualTo(new SpecimenProcessResult(Map.of(), List.of(), List.of()));
		then(digitalMediaService).shouldHaveNoInteractions();
		then(digitalSpecimenService).shouldHaveNoInteractions();
	}

	@Test
	void testEqualSpecimenWithEqualMedia() throws Exception {
		// Given
		given(specimenRepository.getDigitalSpecimens(List.of(PHYSICAL_SPECIMEN_ID)))
			.willReturn(List.of(givenDigitalSpecimenRecord(1, true)));
		given(mediaRepository.getExistingDigitalMedia(Set.of(MEDIA_URL)))
			.willReturn(List.of(givenDigitalMediaRecord()));
		given(entityRelationshipService.processMediaRelationshipsForSpecimen(anyMap(), any(), anyMap()))
			.willReturn(givenEmptyMediaProcessResult());
		given(entityRelationshipService.findNewSpecimenRelationshipsForMedia(any(), any())).willReturn(Set.of());
		given(equalityService.specimensAreEqual(any(), any(), any())).willReturn(true);
		given(equalityService.mediaAreEqual(any(), any(), any())).willReturn(true);
		given(annotationService.applyAcceptedAnnotations(any(), any(), any()))
			.willReturn(givenDigitalSpecimenEvent(true));

		// When
		service.handleMessages(List.of(givenDigitalSpecimenEvent(true)));

		// Then
		then(digitalSpecimenService).should()
			.updateEqualSpecimen(Map.of(givenDigitalSpecimenRecord(1, true), givenDigitalSpecimenEvent(true)));
		then(digitalMediaService).should().updateEqualDigitalMedia(List.of(givenDigitalMediaRecord()));
		then(digitalSpecimenService).shouldHaveNoMoreInteractions();
		then(digitalMediaService).shouldHaveNoMoreInteractions();
		then(pidComponent).shouldHaveNoInteractions();
	}

	@Test
	void testEqualSpecimenWithSingleSpecimenDuplicateMedia() {
		// Given
		var event = new DigitalSpecimenEvent(Set.of(MAS), givenDigitalSpecimenWrapper(false, true),
				List.of(givenDigitalMediaEvent(), givenDigitalMediaEvent()), false, true);

		// When
		service.handleMessages(List.of(event));

		// Then
		then(publisherService).should().deadLetterEventSpecimen(event);
		then(digitalSpecimenService).shouldHaveNoInteractions();
		then(digitalMediaService).shouldHaveNoInteractions();
		then(digitalSpecimenService).shouldHaveNoMoreInteractions();
		then(digitalMediaService).shouldHaveNoMoreInteractions();
		then(pidComponent).shouldHaveNoInteractions();
	}

	@Test
	void testMultipleSpecimenWithDuplicateMedia() throws Exception {
		// Given
		var event = new DigitalSpecimenEvent(Set.of(MAS), givenDigitalSpecimenWrapper(false, true),
				List.of(givenDigitalMediaEvent()), false, true);
		var event2 = new DigitalSpecimenEvent(Set.of(MAS), givenDigitalSpecimenWrapper(PHYSICAL_SPECIMEN_ID_ALT,
				SPECIMEN_NAME, ORGANISATION_ID, false, true, ORIGINAL_DATA), List.of(givenDigitalMediaEvent()), false,
				true);
		given(specimenRepository.getDigitalSpecimens(List.of(PHYSICAL_SPECIMEN_ID))).willReturn(List.of());
		given(mediaRepository.getExistingDigitalMedia(Set.of(MEDIA_URL))).willReturn(List.of());
		given(pidComponent.postPid(any(), eq(true))).willReturn(givenHandleResponseSpecimen());
		given(pidComponent.postPid(any(), eq(false))).willReturn(givenPidResponseMedia());
		given(fdoRecordService.buildPostPidRequest(any())).willReturn(List.of(TestUtils.givenPidRequest()));
		given(fdoRecordService.buildPostRequestMedia(any())).willReturn(List.of(TestUtils.givenPidRequest()));
		given(digitalSpecimenService.createNewDigitalSpecimen(any(), any()))
			.willReturn(Set.of(givenDigitalSpecimenRecord()));
		given(digitalMediaService.createNewDigitalMedia(any(), any())).willReturn(Set.of(givenDigitalMediaRecord()));
		var pidMap = Map.of(PHYSICAL_SPECIMEN_ID, givenPidProcessResultSpecimen(true));
		var pidMapMedia = Map.of(MEDIA_URL, givenPidProcessResultMedia());

		// When
		service.handleMessages(List.of(event, event2));

		// Then
		then(publisherService).should().republishSpecimenEvent(event2);
		then(digitalMediaService).should().createNewDigitalMedia(List.of(givenDigitalMediaEvent()), pidMapMedia);
		then(digitalSpecimenService).should()
			.createNewDigitalSpecimen(List.of(givenDigitalSpecimenEvent(true)), pidMap);
		then(digitalMediaService).should().createNewDigitalMedia(List.of(givenDigitalMediaEvent()), pidMapMedia);
		then(equalityService).shouldHaveNoInteractions();
		then(digitalSpecimenService).shouldHaveNoMoreInteractions();
		then(digitalMediaService).shouldHaveNoMoreInteractions();
		then(masSchedulerService).should().scheduleMasForSpecimen(any());
		then(masSchedulerService).should().scheduleMasForMedia(any());
	}

	@Test
	void testMultipleSpecimenWithDuplicateMediaAndSpecimen() throws Exception {
		// Given
		var event = new DigitalSpecimenEvent(Set.of(MAS), givenDigitalSpecimenWrapper(false, true),
				List.of(givenDigitalMediaEvent()), false, true);
		var event2 = new DigitalSpecimenEvent(Set.of(MAS), givenDigitalSpecimenWrapper(PHYSICAL_SPECIMEN_ID_ALT,
				SPECIMEN_NAME, ORGANISATION_ID, false, true, ORIGINAL_DATA), List.of(givenDigitalMediaEvent()), false,
				true);
		var event3 = new DigitalSpecimenEvent(Set.of(MAS), givenDigitalSpecimenWrapper(PHYSICAL_SPECIMEN_ID_ALT,
				SPECIMEN_NAME, ORGANISATION_ID, false, true, ORIGINAL_DATA),
				List.of(givenDigitalMediaEvent(MEDIA_URL_ALT)), false, true);
		given(specimenRepository.getDigitalSpecimens(List.of(PHYSICAL_SPECIMEN_ID, PHYSICAL_SPECIMEN_ID_ALT)))
			.willReturn(List.of());
		given(mediaRepository.getExistingDigitalMedia(Set.of(MEDIA_URL, MEDIA_URL_ALT))).willReturn(List.of());
		given(pidComponent.postPid(any(), eq(true))).willReturn(givenHandleResponse(
				List.of(PHYSICAL_SPECIMEN_ID, PHYSICAL_SPECIMEN_ID_ALT), List.of(HANDLE, SECOND_HANDLE)));
		given(pidComponent.postPid(any(), eq(false)))
			.willReturn(givenHandleResponse(List.of(MEDIA_URL, MEDIA_URL_ALT), List.of(MEDIA_PID, MEDIA_PID_ALT)));
		given(fdoRecordService.buildPostPidRequest(any()))
			.willReturn(List.of(TestUtils.givenPidRequest(), TestUtils.givenPidRequest()));
		given(fdoRecordService.buildPostRequestMedia(any()))
			.willReturn(List.of(TestUtils.givenPidRequest(), TestUtils.givenPidRequest()));
		given(digitalSpecimenService.createNewDigitalSpecimen(any(), any()))
			.willReturn(Set.of(givenDigitalSpecimenRecord(), givenDigitalSpecimenRecord(SECOND_HANDLE)));
		given(digitalMediaService.createNewDigitalMedia(any(), any()))
			.willReturn(Set.of(givenDigitalMediaRecord(), givenDigitalMediaRecord(SECOND_HANDLE, MEDIA_URL_ALT, 1)));

		// When
		service.handleMessages(List.of(event, event2, event3));

		// Then
		then(publisherService).should().republishSpecimenEvent(event2);
		then(digitalSpecimenService).should().createNewDigitalSpecimen(anyList(), anyMap());
		then(digitalMediaService).should().createNewDigitalMedia(anyList(), anyMap());
		then(equalityService).shouldHaveNoInteractions();
		then(digitalSpecimenService).shouldHaveNoMoreInteractions();
		then(digitalMediaService).shouldHaveNoMoreInteractions();
		then(masSchedulerService).should().scheduleMasForSpecimen(any());
		then(masSchedulerService).should().scheduleMasForMedia(any());
	}

	@Test
	void testChangedSpecimenUpdatedMedia() throws Exception {
		given(specimenRepository.getDigitalSpecimens(List.of(PHYSICAL_SPECIMEN_ID))).willReturn(
				List.of(givenUnequalDigitalSpecimenRecord(HANDLE, ANOTHER_SPECIMEN_NAME, ORGANISATION_ID, true)));
		given(equalityService.specimensAreEqual(any(), any(), any())).willReturn(false);
		given(mediaRepository.getExistingDigitalMedia(any())).willReturn(List.of(givenUnequalDigitalMediaRecord()));
		given(equalityService.setExistingEventDatesSpecimen(any(), any(), any()))
			.willReturn(givenDigitalSpecimenEvent(true));
		given(annotationService.applyAcceptedAnnotations(any(), any(), any()))
			.willReturn(givenDigitalSpecimenEvent(true));
		given(equalityService.setExistingEventDatesMedia(any(), any())).willReturn(givenDigitalMediaEvent());
		given(entityRelationshipService.processMediaRelationshipsForSpecimen(any(), any(), any()))
			.willReturn(givenEmptyMediaProcessResult());
		var pidMapSpecimen = Map.of(PHYSICAL_SPECIMEN_ID, givenPidProcessResultSpecimen(true));

		// When
		service.handleMessages(List.of(givenDigitalSpecimenEvent(true)));

		// Then
		then(digitalSpecimenService).should()
			.updateExistingDigitalSpecimen(
					List.of(givenUpdatedDigitalSpecimenTuple(true, givenEmptyMediaProcessResult())), pidMapSpecimen);
		then(digitalMediaService).should()
			.updateExistingDigitalMedia(List.of(givenUpdatedDigitalMediaTuple(false)), true);
		then(digitalSpecimenService).shouldHaveNoMoreInteractions();
		then(pidComponent).shouldHaveNoInteractions();
		then(fdoRecordService).shouldHaveNoInteractions();
		then(digitalMediaService).shouldHaveNoMoreInteractions();
	}

	@Test
	void testSpecimenAddVirtualCollection() throws Exception {
		given(specimenRepository.getDigitalSpecimens(List.of(PHYSICAL_SPECIMEN_ID))).willReturn(
				List.of(givenUnequalDigitalSpecimenRecord(HANDLE, ANOTHER_SPECIMEN_NAME, ORGANISATION_ID, false)));
		given(equalityService.specimensAreEqual(any(), any(), any())).willReturn(false);
		given(annotationService.applyAcceptedAnnotations(any(), any(), any())).willReturn(givenDigitalSpecimenEvent());
		var pidMapSpecimen = Map.of(PHYSICAL_SPECIMEN_ID, givenPidProcessResultSpecimen(false));
		var digitalSpecimen = givenDigitalSpecimenWrapper(false, false);
		digitalSpecimen.attributes()
			.setOdsHasEntityRelationships(List.of(givenEntityRelationship(),
					new EntityRelationship().withId("https://hdl.handle.net/20.5000.1025/V1Z-176-VCL")
						.withOdsRelatedResourceURI(URI.create("https://hdl.handle.net/20.5000.1025/V1Z-176-VCL"))
						.withDwcRelationshipOfResource("hasVirtualCollection")));
		var digitalSpecimenEvent = new DigitalSpecimenEvent(Set.of(MAS), digitalSpecimen, List.of(), false, false);
		given(equalityService.setExistingEventDatesSpecimen(any(), any(), any())).willReturn(digitalSpecimenEvent);

		// When
		service.handleMessages(List.of(digitalSpecimenEvent));

		// Then
		then(digitalSpecimenService).should()
			.updateExistingDigitalSpecimen(List.of(new UpdatedDigitalSpecimenTuple(
					givenUnequalDigitalSpecimenRecord(HANDLE, ANOTHER_SPECIMEN_NAME, ORGANISATION_ID, false, false),
					digitalSpecimenEvent, givenEmptyMediaProcessResult())), pidMapSpecimen);
		then(digitalMediaService).shouldHaveNoInteractions();
		then(digitalSpecimenService).shouldHaveNoMoreInteractions();
		then(pidComponent).shouldHaveNoInteractions();
		then(fdoRecordService).shouldHaveNoInteractions();
		then(digitalMediaService).shouldHaveNoMoreInteractions();
	}

	@Test
	void testChangedSpecimenUpdatedMediaSharedMedia() throws Exception {
		var event1 = givenDigitalSpecimenEvent(true);
		var record1 = givenDigitalSpecimenRecordWithMediaEr();
		given(specimenRepository.getDigitalSpecimens(any())).willReturn(List.of(record1));
		given(equalityService.specimensAreEqual(any(), any(), any())).willReturn(false);
		given(mediaRepository.getExistingDigitalMedia(Set.of(MEDIA_URL)))
			.willReturn(List.of(givenUnequalDigitalMediaRecord()));
		given(equalityService.setExistingEventDatesSpecimen(any(), eq(event1), any())).willReturn(event1);
		given(equalityService.setExistingEventDatesMedia(any(), any())).willReturn(givenDigitalMediaEvent());

		given(entityRelationshipService.processMediaRelationshipsForSpecimen(any(), any(), any()))
			.willReturn(givenEmptyMediaProcessResult());
		given(annotationService.applyAcceptedAnnotations(any(), any(), any()))
			.willReturn(givenDigitalSpecimenEvent(true));
		var pidMapSpecimen = Map.of(PHYSICAL_SPECIMEN_ID, givenPidProcessResultSpecimen(true));

		// When
		service.handleMessages(List.of(event1));

		// Then
		then(digitalSpecimenService).should().updateExistingDigitalSpecimen(any(), eq(pidMapSpecimen));
		then(digitalMediaService).should()
			.updateExistingDigitalMedia(List.of(givenUpdatedDigitalMediaTuple(false)), true);
		then(digitalSpecimenService).shouldHaveNoMoreInteractions();
		then(digitalMediaService).shouldHaveNoMoreInteractions();
		then(pidComponent).shouldHaveNoInteractions();
		then(fdoRecordService).shouldHaveNoInteractions();
	}

	@Test
	void testNewSpecimenNewMedia() throws Exception {
		given(specimenRepository.getDigitalSpecimens(List.of(PHYSICAL_SPECIMEN_ID))).willReturn(List.of());
		given(mediaRepository.getExistingDigitalMedia(Set.of(MEDIA_URL))).willReturn(List.of());
		given(pidComponent.postPid(any(), eq(true))).willReturn(givenHandleResponseSpecimen());
		given(pidComponent.postPid(any(), eq(false))).willReturn(givenPidResponseMedia());
		given(fdoRecordService.buildPostPidRequest(any())).willReturn(List.of(TestUtils.givenPidRequest()));
		given(fdoRecordService.buildPostRequestMedia(any())).willReturn(List.of(TestUtils.givenPidRequest()));
		given(digitalSpecimenService.createNewDigitalSpecimen(any(), any()))
			.willReturn(Set.of(givenDigitalSpecimenRecord()));
		given(digitalMediaService.createNewDigitalMedia(any(), any())).willReturn(Set.of(givenDigitalMediaRecord()));
		var pidMap = Map.of(PHYSICAL_SPECIMEN_ID, givenPidProcessResultSpecimen(true));
		var pidMapMedia = Map.of(MEDIA_URL, givenPidProcessResultMedia());

		// When
		service.handleMessages(List.of(givenDigitalSpecimenEvent(true)));

		// Then
		then(digitalMediaService).should().createNewDigitalMedia(List.of(givenDigitalMediaEvent()), pidMapMedia);
		then(digitalSpecimenService).should()
			.createNewDigitalSpecimen(List.of(givenDigitalSpecimenEvent(true)), pidMap);
		then(digitalMediaService).should().createNewDigitalMedia(List.of(givenDigitalMediaEvent()), pidMapMedia);
		then(equalityService).shouldHaveNoInteractions();
		then(digitalSpecimenService).shouldHaveNoMoreInteractions();
		then(digitalMediaService).shouldHaveNoMoreInteractions();
		then(masSchedulerService).should().scheduleMasForSpecimen(any());
		then(masSchedulerService).should().scheduleMasForMedia(any());
	}

	@Test
	void testNewSpecimenTooManyNewMedia() {
		// Given
		var mediaEvents = new ArrayList<DigitalMediaEvent>();
		IntStream.rangeClosed(0, 10000)
			.forEach(i -> mediaEvents.add(givenDigitalMediaEvent(UUID.randomUUID().toString())));
		var specimenEvents = List
			.of(new DigitalSpecimenEvent(Set.of(), givenDigitalSpecimenWrapper(), mediaEvents, false, true));

		// When / Then
		assertThrows(TooManyObjectsException.class, () -> service.handleMessages(specimenEvents));
	}

	@Test
	void testNewSpecimenNewMediaBatchedPid() throws Exception {
		given(specimenRepository.getDigitalSpecimens(any())).willReturn(List.of());
		given(mediaRepository.getExistingDigitalMedia(any())).willReturn(List.of());
		given(pidComponent.postPid(any(), eq(true))).willReturn(givenHandleResponseSpecimen());
		given(fdoRecordService.buildPostPidRequest(any())).willReturn(List.of(TestUtils.givenPidRequest()));
		given(fdoRecordService.buildPostRequestMedia(any())).willReturn(List.of(TestUtils.givenPidRequest()));
		var mediaEvents = new ArrayList<DigitalMediaEvent>();
		IntStream.rangeClosed(0, 2002)
			.forEach(i -> mediaEvents.add(givenDigitalMediaEvent(UUID.randomUUID().toString())));
		var specimenEvents = List
			.of(new DigitalSpecimenEvent(Set.of(), givenDigitalSpecimenWrapper(false, true), mediaEvents, false, true));

		// When
		service.handleMessages(specimenEvents);

		// Then
		then(pidComponent).should(times(4)).postPid(any(), anyBoolean());
	}

}
