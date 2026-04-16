package eu.dissco.core.digitalspecimenprocessor.service.preprocessing;

import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.CREATED;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MAPPER;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MEDIA_MAS;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MEDIA_PID;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MEDIA_URL;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalMedia;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalMediaEvent;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalMediaRecord;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;

import eu.dissco.core.digitalspecimenprocessor.domain.FdoType;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaEvent;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaWrapper;
import eu.dissco.core.digitalspecimenprocessor.domain.media.MediaProcessResult;
import eu.dissco.core.digitalspecimenprocessor.domain.relation.PidProcessResult;
import eu.dissco.core.digitalspecimenprocessor.property.ApplicationProperties;
import eu.dissco.core.digitalspecimenprocessor.repository.DigitalMediaRepository;
import eu.dissco.core.digitalspecimenprocessor.service.DigitalMediaService;
import eu.dissco.core.digitalspecimenprocessor.service.EntityRelationshipService;
import eu.dissco.core.digitalspecimenprocessor.service.EqualityService;
import eu.dissco.core.digitalspecimenprocessor.service.FdoRecordService;
import eu.dissco.core.digitalspecimenprocessor.service.MasSchedulerService;
import eu.dissco.core.digitalspecimenprocessor.service.RabbitMqPublisherService;
import eu.dissco.core.digitalspecimenprocessor.web.PidComponent;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
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
class MediaPreprocessingServiceTest {

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
	private MasSchedulerService masSchedulerService;

	private MockedStatic<Instant> mockedInstant;

	private MediaPreprocessingService service;

	@BeforeEach
	void setup() {
		service = new MediaPreprocessingService(MAPPER, mediaRepository, digitalMediaService, publisherService,
				entityRelationshipService, equalityService, new ApplicationProperties(), fdoRecordService, pidComponent,
				masSchedulerService);
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
	void testHandleMessagesMediaNew() throws Exception {
		// Given
		given(pidComponent.postPid(any(), eq(false))).willReturn(Map.of(MEDIA_URL, MEDIA_PID));
		var events = List.of(givenDigitalMediaEvent());
		given(digitalMediaService.createNewDigitalMedia(events,
				Map.of(MEDIA_URL, new PidProcessResult(MEDIA_PID, Set.of()))))
			.willReturn(Set.of(givenDigitalMediaRecord()));

		// When
		var result = service.handleMessagesMedia(events);

		// Then
		assertThat(result).isEqualTo(new MediaProcessResult(List.of(), List.of(), List.of(givenDigitalMediaRecord())));
		then(pidComponent).shouldHaveNoMoreInteractions();
		then(digitalMediaService).shouldHaveNoMoreInteractions();
		then(masSchedulerService).should().scheduleMasForMedia(any());
		then(masSchedulerService).shouldHaveNoMoreInteractions();
	}

	@Test
	void testHandleMessagesMediaEqual() {
		// Given
		var events = List.of(givenDigitalMediaEvent());
		given(mediaRepository.getExistingDigitalMedia(Set.of(MEDIA_URL)))
			.willReturn(List.of(givenDigitalMediaRecord()));
		given(equalityService.mediaAreEqual(givenDigitalMediaRecord(), givenDigitalMediaEvent().digitalMediaWrapper(),
				Set.of()))
			.willReturn(true);

		// When
		var result = service.handleMessagesMedia(events);

		// Then
		assertThat(result).isEqualTo(new MediaProcessResult(List.of(givenDigitalMediaRecord()), List.of(), List.of()));
		then(pidComponent).shouldHaveNoInteractions();
		then(digitalMediaService).should().updateEqualDigitalMedia(List.of(givenDigitalMediaRecord()));
		then(digitalMediaService).shouldHaveNoMoreInteractions();
	}

	@Test
	void testHandleMessagesMediaUpdate() {
		// Given
		var events = List.of(givenDigitalMediaEvent());
		given(mediaRepository.getExistingDigitalMedia(Set.of(MEDIA_URL)))
			.willReturn(List.of(givenDigitalMediaRecord()));
		given(equalityService.mediaAreEqual(givenDigitalMediaRecord(), givenDigitalMediaEvent().digitalMediaWrapper(),
				Set.of()))
			.willReturn(false);
		given(digitalMediaService.updateExistingDigitalMedia(any(), eq(true)))
			.willReturn(Set.of(givenDigitalMediaRecord()));

		// When
		var result = service.handleMessagesMedia(events);

		// Then
		assertThat(result).isEqualTo(new MediaProcessResult(List.of(), List.of(givenDigitalMediaRecord()), List.of()));
		then(pidComponent).shouldHaveNoInteractions();
		then(digitalMediaService).shouldHaveNoMoreInteractions();
	}

	@Test
	void testHandleMessagesMediaEqualDuplicatesNoChanges() {
		// Given
		var events = List.of(givenDigitalMediaEvent(), givenDigitalMediaEvent());
		given(mediaRepository.getExistingDigitalMedia(Set.of(MEDIA_URL)))
			.willReturn(List.of(givenDigitalMediaRecord()));
		given(equalityService.mediaAreEqual(givenDigitalMediaRecord(), givenDigitalMediaEvent().digitalMediaWrapper(),
				Set.of()))
			.willReturn(true);

		// When
		var result = service.handleMessagesMedia(events);

		// Then
		assertThat(result).isEqualTo(new MediaProcessResult(List.of(givenDigitalMediaRecord()), List.of(), List.of()));
		then(pidComponent).shouldHaveNoInteractions();
		then(digitalMediaService).should().updateEqualDigitalMedia(List.of(givenDigitalMediaRecord()));
		then(digitalMediaService).shouldHaveNoMoreInteractions();
		then(publisherService).shouldHaveNoMoreInteractions();
	}

	@Test
	void testHandleMessagesMediaEqualDuplicatesWithChanges() {
		// Given
		var secondEvent = new DigitalMediaEvent(Set.of(MEDIA_MAS),
				new DigitalMediaWrapper(FdoType.MEDIA.getPid(),
						givenDigitalMedia(MEDIA_URL, false).withAcCaptureDevice("a camera"), MAPPER.createObjectNode()),
				false, true);
		var events = new ArrayList<DigitalMediaEvent>();
		events.add(givenDigitalMediaEvent());
		events.add(secondEvent);
		given(mediaRepository.getExistingDigitalMedia(Set.of(MEDIA_URL)))
			.willReturn(List.of(givenDigitalMediaRecord()));
		given(equalityService.mediaAreEqual(givenDigitalMediaRecord(), givenDigitalMediaEvent().digitalMediaWrapper(),
				Set.of()))
			.willReturn(true);

		// When
		var result = service.handleMessagesMedia(events);

		// Then
		assertThat(result).isEqualTo(new MediaProcessResult(List.of(givenDigitalMediaRecord()), List.of(), List.of()));
		then(pidComponent).shouldHaveNoInteractions();
		then(digitalMediaService).should().updateEqualDigitalMedia(List.of(givenDigitalMediaRecord()));
		then(digitalMediaService).shouldHaveNoMoreInteractions();
		then(publisherService).should(times(1)).republishMediaEvent(secondEvent);
	}

}
