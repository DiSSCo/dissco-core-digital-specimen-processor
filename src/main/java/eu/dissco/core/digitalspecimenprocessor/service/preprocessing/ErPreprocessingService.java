package eu.dissco.core.digitalspecimenprocessor.service.preprocessing;

import static eu.dissco.core.digitalspecimenprocessor.util.DigitalObjectUtils.DOI_PROXY;

import eu.dissco.core.digitalspecimenprocessor.Profiles;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaEvent;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaRecord;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaWrapper;
import eu.dissco.core.digitalspecimenprocessor.domain.media.UpdatedDigitalMediaTuple;
import eu.dissco.core.digitalspecimenprocessor.domain.relation.DigitalMediaRelationshipTombstoneEvent;
import eu.dissco.core.digitalspecimenprocessor.property.ApplicationProperties;
import eu.dissco.core.digitalspecimenprocessor.repository.DigitalMediaRepository;
import eu.dissco.core.digitalspecimenprocessor.schema.DigitalMedia;
import eu.dissco.core.digitalspecimenprocessor.schema.EntityRelationship;
import eu.dissco.core.digitalspecimenprocessor.service.DigitalMediaService;
import eu.dissco.core.digitalspecimenprocessor.service.EntityRelationshipService;
import eu.dissco.core.digitalspecimenprocessor.service.EqualityService;
import eu.dissco.core.digitalspecimenprocessor.service.FdoRecordService;
import eu.dissco.core.digitalspecimenprocessor.service.RabbitMqPublisherService;
import eu.dissco.core.digitalspecimenprocessor.web.PidComponent;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;
import tools.jackson.databind.json.JsonMapper;

@Service
@Slf4j
@Profile(Profiles.ER_RABBIT_MQ)
public class ErPreprocessingService extends AbstractPreprocessingService {

	ErPreprocessingService(JsonMapper jsonMapper, DigitalMediaRepository mediaRepository,
			DigitalMediaService digitalMediaService, RabbitMqPublisherService rabbitMqPublisherService,
			EntityRelationshipService entityRelationshipService, EqualityService equalityService,
			ApplicationProperties applicationProperties, FdoRecordService fdoRecordService, PidComponent pidComponent) {
		super(jsonMapper, mediaRepository, digitalMediaService, rabbitMqPublisherService, entityRelationshipService,
				equalityService, applicationProperties, fdoRecordService, pidComponent);
	}

	public void handleMessagesMediaRelationshipTombstone(List<DigitalMediaRelationshipTombstoneEvent> events) {
		log.info("Processing {} digital media relationship tombstone events", events.size());
		var uniqueEvents = uniqueMediaRelationshipTombstoneEvents(events);
		var mediaDois = uniqueEvents.stream()
			.map(DigitalMediaRelationshipTombstoneEvent::mediaDoi)
			.collect(Collectors.toSet());
		var currentDigitalMediaRecords = mediaRepository.getExistingDigitalMediaByDoi(mediaDois)
			.stream()
			.collect(Collectors.toMap(DigitalMediaRecord::id, Function.identity()));
		var updatedDigitalMediaTuples = uniqueEvents.stream()
			.map(event -> createDigitalMediaEventWithoutER(event, currentDigitalMediaRecords))
			.filter(Optional::isPresent)
			.map(Optional::get)
			.toList();
		if (updatedDigitalMediaTuples.isEmpty()) {
			log.info("No media relationships to tombstone");
			return;
		}
		log.info("Relationships removed for: {} digital media objects, processing updates",
				updatedDigitalMediaTuples.size());
		digitalMediaService.updateExistingDigitalMedia(updatedDigitalMediaTuples, false);
	}

	private List<DigitalMediaRelationshipTombstoneEvent> uniqueMediaRelationshipTombstoneEvents(
			List<DigitalMediaRelationshipTombstoneEvent> events) {
		var uniqueSet = new LinkedHashSet<DigitalMediaRelationshipTombstoneEvent>();
		var map = events.stream()
			.filter(ErPreprocessingService::mediaIsNotNull)
			.collect(Collectors.groupingBy(DigitalMediaRelationshipTombstoneEvent::mediaDoi));
		for (var entry : map.entrySet()) {
			if (entry.getValue().size() > 1) {
				log.warn("Found {} duplicate media relationship tombstone events in batch for media id {}",
						entry.getValue().size(), entry.getKey());
				for (int i = 0; i < entry.getValue().size(); i++) {
					if (i == 0) {
						uniqueSet.add(entry.getValue().get(i));
					}
					else {
						republishMediaRelationshipTombstoneEvent(entry.getValue().get(i));
					}
				}
			}
			else {
				uniqueSet.add(entry.getValue().getFirst());
			}
		}
		return new ArrayList<>(uniqueSet);
	}

	private Optional<UpdatedDigitalMediaTuple> createDigitalMediaEventWithoutER(
			DigitalMediaRelationshipTombstoneEvent event, Map<String, DigitalMediaRecord> existingMedia) {
		var currentDigitalMediaRecord = existingMedia.get(event.mediaDoi());
		var updatedDigitalMediaEvent = generatedUpdatedMediaEvent(event, currentDigitalMediaRecord);
		if (Objects.equals(currentDigitalMediaRecord.attributes(),
				updatedDigitalMediaEvent.digitalMediaWrapper().attributes())) {
			log.warn("No change in digital media: {} after removing relationship to specimen {}", event.mediaDoi(),
					event.specimenDoi());
			return Optional.empty();
		}
		return Optional.of(new UpdatedDigitalMediaTuple(currentDigitalMediaRecord, updatedDigitalMediaEvent,
				Collections.emptySet()));
	}

	private DigitalMediaEvent generatedUpdatedMediaEvent(DigitalMediaRelationshipTombstoneEvent event,
			DigitalMediaRecord currentDigitalMediaRecord) {
		var updatedDigitalMediaAttributes = deepCopy(currentDigitalMediaRecord.attributes());
		updatedDigitalMediaAttributes
			.setOdsHasEntityRelationships(removeRelationship(event, updatedDigitalMediaAttributes));
		return new DigitalMediaEvent(Collections.emptySet(), new DigitalMediaWrapper(
				updatedDigitalMediaAttributes.getOdsFdoType(), updatedDigitalMediaAttributes, null), false, false);
	}

	private void republishMediaRelationshipTombstoneEvent(DigitalMediaRelationshipTombstoneEvent event) {
		publisherService.publishDigitalMediaRelationTombstone(event);
	}

	/*
	 * In the exceptional case that the media object is null due to an issue earlier in
	 * the pipeline, we log a warning and skip The media relationship should be removed
	 * from the DigitalSpecimen, however there is no Media object to update
	 */
	private static boolean mediaIsNotNull(DigitalMediaRelationshipTombstoneEvent event) {
		if (event.mediaDoi() == null || event.mediaDoi().isBlank() || event.mediaDoi().equals("null")) {
			log.warn(
					"Received media relationship tombstone event with empty media DOI for specimen: {}, skipping event",
					event.specimenDoi());
			return false;
		}
		else {
			return true;
		}
	}

	private DigitalMedia deepCopy(DigitalMedia currentDigitalMedia) {
		return objectMapper.readValue(objectMapper.writeValueAsString(currentDigitalMedia), DigitalMedia.class);
	}

	private static List<EntityRelationship> removeRelationship(DigitalMediaRelationshipTombstoneEvent event,
			DigitalMedia updatedMedia) {
		var newEntityRelationships = new ArrayList<EntityRelationship>();
		updatedMedia.getOdsHasEntityRelationships()
			.stream()
			.filter(er -> !er.getOdsRelatedResourceURI().toString().equals(DOI_PROXY + event.specimenDoi()))
			.forEach(newEntityRelationships::add);
		return newEntityRelationships;
	}

}
