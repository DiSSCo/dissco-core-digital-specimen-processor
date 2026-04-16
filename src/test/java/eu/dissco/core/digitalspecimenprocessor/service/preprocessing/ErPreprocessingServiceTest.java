package eu.dissco.core.digitalspecimenprocessor.service.preprocessing;

import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.CREATED;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.HANDLE;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MAPPER;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MEDIA_PID;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MEDIA_URL;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.SECOND_HANDLE;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalMedia;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalMediaRecord;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalMediaTombstoneEvent;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mockStatic;

import eu.dissco.core.digitalspecimenprocessor.domain.FdoType;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaEvent;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaWrapper;
import eu.dissco.core.digitalspecimenprocessor.domain.media.UpdatedDigitalMediaTuple;
import eu.dissco.core.digitalspecimenprocessor.domain.relation.DigitalMediaRelationshipTombstoneEvent;
import eu.dissco.core.digitalspecimenprocessor.property.ApplicationProperties;
import eu.dissco.core.digitalspecimenprocessor.repository.DigitalMediaRepository;
import eu.dissco.core.digitalspecimenprocessor.service.DigitalMediaService;
import eu.dissco.core.digitalspecimenprocessor.service.EntityRelationshipService;
import eu.dissco.core.digitalspecimenprocessor.service.EqualityService;
import eu.dissco.core.digitalspecimenprocessor.service.FdoRecordService;
import eu.dissco.core.digitalspecimenprocessor.service.RabbitMqPublisherService;
import eu.dissco.core.digitalspecimenprocessor.web.PidComponent;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ErPreprocessingServiceTest {

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

	private MockedStatic<Instant> mockedInstant;

	private ErPreprocessingService service;

	@BeforeEach
	void setup() {
		service = new ErPreprocessingService(MAPPER, mediaRepository, digitalMediaService, publisherService,
				entityRelationshipService, equalityService, new ApplicationProperties(), fdoRecordService,
				pidComponent);
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
	void testHandleMessageMediaRelationshipTombstone() {
		// Given
		var event = givenDigitalMediaTombstoneEvent();
		var duplicateEvent = new DigitalMediaRelationshipTombstoneEvent(SECOND_HANDLE, MEDIA_PID);
		given(mediaRepository.getExistingDigitalMediaByDoi(Set.of(MEDIA_PID)))
			.willReturn(List.of(givenDigitalMediaRecord()));
		var digitalMediaEvent = new DigitalMediaEvent(Set.of(),
				new DigitalMediaWrapper(FdoType.MEDIA.getPid(), givenDigitalMedia(MEDIA_URL, false), null), false,
				false);
		digitalMediaEvent.digitalMediaWrapper().attributes().setOdsHasEntityRelationships(List.of());
		var updatedMediaTuple = new UpdatedDigitalMediaTuple(givenDigitalMediaRecord(), digitalMediaEvent,
				Collections.emptySet());

		// When
		service.handleMessagesMediaRelationshipTombstone(List.of(event, duplicateEvent));

		// Then
		then(publisherService).should().publishDigitalMediaRelationTombstone(duplicateEvent);
		then(digitalMediaService).should().updateExistingDigitalMedia(List.of(updatedMediaTuple), false);
	}

	@ParameterizedTest
	@ValueSource(strings = { "null", " " })
	@NullAndEmptySource
	void testHandleMessageMediaRelationshipTombstoneNullMediaDoi(String mediaDoi) {
		// Given
		var event = new DigitalMediaRelationshipTombstoneEvent(HANDLE, mediaDoi);
		given(mediaRepository.getExistingDigitalMediaByDoi(Set.of())).willReturn(List.of());

		// When
		service.handleMessagesMediaRelationshipTombstone(List.of(event));

		// Then
		then(publisherService).shouldHaveNoInteractions();
		then(digitalMediaService).shouldHaveNoInteractions();
	}

	@Test
	void testHandleMessageMediaRelationshipTombstoneNoChange() {
		// Given
		var event = new DigitalMediaRelationshipTombstoneEvent(SECOND_HANDLE, MEDIA_PID);
		given(mediaRepository.getExistingDigitalMediaByDoi(Set.of(MEDIA_PID)))
			.willReturn(List.of(givenDigitalMediaRecord()));

		// When
		service.handleMessagesMediaRelationshipTombstone(List.of(event));

		// Then
		then(digitalMediaService).shouldHaveNoInteractions();
	}

}
