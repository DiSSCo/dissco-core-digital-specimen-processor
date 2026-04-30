package eu.dissco.core.digitalspecimenprocessor.service.preprocessing;

import static eu.dissco.core.digitalspecimenprocessor.util.DigitalObjectUtils.DOI_PROXY;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toMap;

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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
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
		var currentDigitalMediaRecords = mediaRepository.getExistingDigitalMediaByDoi(uniqueEvents.keySet())
			.stream()
			.collect(Collectors.toMap(DigitalMediaRecord::id, Function.identity()));
		var updatedDigitalMediaTuples = uniqueEvents.entrySet()
			.stream()
			.map(entry -> createDigitalMediaEventWithoutER(entry.getKey(), entry.getValue(),
					currentDigitalMediaRecords))
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

	private Map<String, Set<String>> uniqueMediaRelationshipTombstoneEvents(
			List<DigitalMediaRelationshipTombstoneEvent> events) {
		return events.stream()
			.filter(ErPreprocessingService::mediaIsNotNull)
			.collect(groupingBy(DigitalMediaRelationshipTombstoneEvent::mediaDoi))
			.entrySet()
			.stream()
			.collect(toMap(Entry::getKey,
					e -> e.getValue()
						.stream()
						.map(DigitalMediaRelationshipTombstoneEvent::specimenDoi)
						.collect(Collectors.toSet())));
	}

	private Optional<UpdatedDigitalMediaTuple> createDigitalMediaEventWithoutER(String mediaDoi,
			Set<String> specimenDois, Map<String, DigitalMediaRecord> existingMedia) {
		var currentDigitalMediaRecord = existingMedia.get(mediaDoi);
		var updatedDigitalMediaEvent = generatedUpdatedMediaEvent(specimenDois, currentDigitalMediaRecord);
		if (Objects.equals(currentDigitalMediaRecord.attributes(),
				updatedDigitalMediaEvent.digitalMediaWrapper().attributes())) {
			log.warn("No change in digital media: {} after removing relationship to specimen(s) {}", mediaDoi,
					specimenDois);
			return Optional.empty();
		}
		return Optional.of(new UpdatedDigitalMediaTuple(currentDigitalMediaRecord, updatedDigitalMediaEvent,
				Collections.emptySet()));
	}

	private DigitalMediaEvent generatedUpdatedMediaEvent(Set<String> specimenDois,
			DigitalMediaRecord currentDigitalMediaRecord) {
		var updatedDigitalMediaAttributes = deepCopy(currentDigitalMediaRecord.attributes());
		updatedDigitalMediaAttributes
			.setOdsHasEntityRelationships(removeRelationships(specimenDois, updatedDigitalMediaAttributes));
		return new DigitalMediaEvent(Collections.emptySet(), new DigitalMediaWrapper(
				updatedDigitalMediaAttributes.getOdsFdoType(), updatedDigitalMediaAttributes, null), false, false);
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

	private static List<EntityRelationship> removeRelationships(Set<String> speicmenDois, DigitalMedia updatedMedia) {
		var specimenDoisWithProxy = speicmenDois.stream().map(doi -> DOI_PROXY + doi).collect(Collectors.toSet());
		return updatedMedia.getOdsHasEntityRelationships()
			.stream()
			.filter(er -> !specimenDoisWithProxy.contains(er.getOdsRelatedResourceURI().toString()))
			.toList();
	}

}
