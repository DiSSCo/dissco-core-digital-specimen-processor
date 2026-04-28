package eu.dissco.core.digitalspecimenprocessor.service.preprocessing;

import static java.util.stream.Collectors.toMap;

import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaEvent;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaRecord;
import eu.dissco.core.digitalspecimenprocessor.domain.media.MediaPreprocessResult;
import eu.dissco.core.digitalspecimenprocessor.domain.media.MediaProcessResult;
import eu.dissco.core.digitalspecimenprocessor.domain.media.UpdatedDigitalMediaTuple;
import eu.dissco.core.digitalspecimenprocessor.domain.relation.PidProcessResult;
import eu.dissco.core.digitalspecimenprocessor.exception.PidException;
import eu.dissco.core.digitalspecimenprocessor.property.ApplicationProperties;
import eu.dissco.core.digitalspecimenprocessor.repository.DigitalMediaRepository;
import eu.dissco.core.digitalspecimenprocessor.service.DigitalMediaService;
import eu.dissco.core.digitalspecimenprocessor.service.EntityRelationshipService;
import eu.dissco.core.digitalspecimenprocessor.service.EqualityService;
import eu.dissco.core.digitalspecimenprocessor.service.FdoRecordService;
import eu.dissco.core.digitalspecimenprocessor.service.RabbitMqPublisherService;
import eu.dissco.core.digitalspecimenprocessor.web.PidComponent;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

@Service
@RequiredArgsConstructor
@Slf4j
public abstract class AbstractPreprocessingService {

	protected final JsonMapper objectMapper;

	protected final DigitalMediaRepository mediaRepository;

	protected final DigitalMediaService digitalMediaService;

	protected final RabbitMqPublisherService publisherService;

	protected final EntityRelationshipService entityRelationshipService;

	protected final EqualityService equalityService;

	protected final ApplicationProperties applicationProperties;

	protected final FdoRecordService fdoRecordService;

	private final PidComponent pidComponent;

	protected Map<String, DigitalMediaRecord> getCurrentMedia(Set<DigitalMediaEvent> mediaEvents) {
		var mediaURIs = mediaEvents.stream()
			.map(mediaEvent -> mediaEvent.digitalMediaWrapper().attributes().getAcAccessURI())
			.collect(Collectors.toSet());
		var eventMap = mediaEvents.stream()
			.collect(Collectors.toMap(event -> event.digitalMediaWrapper().attributes().getAcAccessURI(),
					Function.identity()));
		if (!mediaURIs.isEmpty()) {
			return mediaRepository.getExistingDigitalMedia(mediaURIs)
				.stream()
				.filter(Objects::nonNull)
				.map(dbRecord -> {
					var event = eventMap.get(dbRecord.accessURI());
					return new DigitalMediaRecord(dbRecord.id(), dbRecord.accessURI(), dbRecord.version(),
							dbRecord.created(), event.masList(), dbRecord.attributes(), dbRecord.originalAttributes(),
							event.forceMasSchedule(), event.isDataFromSourceSystem());
				})
				.collect(toMap(DigitalMediaRecord::accessURI, Function.identity(), (uri1, uri2) -> {
					log.warn("Duplicate URIs found for digital media");
					return uri1;
				}));
		}
		return Map.of();
	}

	protected MediaPreprocessResult preprocessMedia(Set<DigitalMediaEvent> events,
			Map<String, DigitalMediaRecord> currentDigitalMedias, Map<String, PidProcessResult> pidMap) {
		var equalDigitalMedia = new ArrayList<DigitalMediaRecord>();
		var changedDigitalMedia = new ArrayList<UpdatedDigitalMediaTuple>();
		var newDigitalMedia = new ArrayList<DigitalMediaEvent>();
		for (var mediaEvent : events) {
			var digitalMedia = mediaEvent.digitalMediaWrapper();
			var accessUri = digitalMedia.attributes().getAcAccessURI();
			log.debug("Processing digitalMediaWrapper: {}", digitalMedia);
			if (!currentDigitalMedias.containsKey(accessUri)) {
				log.debug("DigitalMedia with uri: {} is completely new", accessUri);
				newDigitalMedia.add(mediaEvent);
			}
			else {
				var currentDigitalMedia = currentDigitalMedias.get(accessUri);
				var relatedSpecimenDois = entityRelationshipService
					.findNewSpecimenRelationshipsForMedia(currentDigitalMedia, pidMap.get(accessUri));
				if (equalityService.mediaAreEqual(currentDigitalMedia, digitalMedia, relatedSpecimenDois)) {
					log.debug("Received digital media is equal to digital media: {}", currentDigitalMedia.id());
					equalDigitalMedia.add(currentDigitalMedia);
				}
				else {
					var eventWithUpdatedEr = equalityService.setExistingEventDatesMedia(currentDigitalMedia,
							mediaEvent);
					log.debug("Digital Media Object with id: {} has received an update", currentDigitalMedia.id());
					changedDigitalMedia.add(
							new UpdatedDigitalMediaTuple(currentDigitalMedia, eventWithUpdatedEr, relatedSpecimenDois));
				}
			}
		}
		return new MediaPreprocessResult(equalDigitalMedia, changedDigitalMedia, newDigitalMedia);
	}

	protected MediaProcessResult processMedia(MediaPreprocessResult mediaPreprocessResult,
			Map<String, PidProcessResult> pidProcessResults) {
		var equalMedia = new ArrayList<DigitalMediaRecord>();
		var updatedMedia = new ArrayList<DigitalMediaRecord>();
		var newMedia = new ArrayList<DigitalMediaRecord>();
		if (!mediaPreprocessResult.equalDigitalMedia().isEmpty()) {
			digitalMediaService.updateEqualDigitalMedia(mediaPreprocessResult.equalDigitalMedia());
			equalMedia = new ArrayList<>(mediaPreprocessResult.equalDigitalMedia());
		}
		if (!mediaPreprocessResult.newDigitalMedia().isEmpty()) {
			newMedia = new ArrayList<>(digitalMediaService
				.createNewDigitalMedia(mediaPreprocessResult.newDigitalMedia(), pidProcessResults));
		}
		if (!mediaPreprocessResult.changedDigitalMedia().isEmpty()) {
			updatedMedia = new ArrayList<>(
					digitalMediaService.updateExistingDigitalMedia(mediaPreprocessResult.changedDigitalMedia(), true));
		}
		return new MediaProcessResult(equalMedia, updatedMedia, newMedia);
	}

	protected Map<String, String> createPidsForNewMediaObjects(Map<String, DigitalMediaRecord> existingMedia,
			Set<DigitalMediaEvent> digitalMediaEvents) {
		var newEvents = digitalMediaEvents.stream()
			.filter(e -> !existingMedia.containsKey(e.digitalMediaWrapper().attributes().getAcAccessURI()))
			.toList();
		return createNewMediaPids(newEvents);
	}

	private Map<String, String> createNewMediaPids(List<DigitalMediaEvent> digitalMediaEvents) {
		if (digitalMediaEvents.isEmpty()) {
			return Map.of();
		}
		var pidMap = new HashMap<String, String>();
		for (int i = 0; i < digitalMediaEvents.size(); i += applicationProperties.getMaxPids()) {
			int j = Math.min(i + applicationProperties.getMaxPids(), digitalMediaEvents.size());
			var sublist = digitalMediaEvents.subList(i, j);
			var request = fdoRecordService.buildPostRequestMedia(sublist);
			pidMap.putAll(createNewPids(request, false));
		}
		return pidMap;
	}

	protected Map<String, String> createNewPids(List<JsonNode> request, boolean isSpecimen) {
		try {
			var pidMap = pidComponent.postPid(request, isSpecimen);
			log.info("Successfully minted {} {} PIDs", pidMap.size(), isSpecimen ? "specimen" : "media");
			return pidMap; // map localId : doiOfTarget
		}
		catch (PidException e) {
			log.error("Unable to create new PIDs", e);
		}
		return Map.of();
	}

}
