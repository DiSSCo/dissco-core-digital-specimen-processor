package eu.dissco.core.digitalspecimenprocessor.service.preprocessing;

import eu.dissco.core.digitalspecimenprocessor.Profiles;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaEvent;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaRecord;
import eu.dissco.core.digitalspecimenprocessor.domain.media.MediaProcessResult;
import eu.dissco.core.digitalspecimenprocessor.domain.relation.PidProcessResult;
import eu.dissco.core.digitalspecimenprocessor.exception.TooManyObjectsException;
import eu.dissco.core.digitalspecimenprocessor.property.ApplicationProperties;
import eu.dissco.core.digitalspecimenprocessor.repository.DigitalMediaRepository;
import eu.dissco.core.digitalspecimenprocessor.service.DigitalMediaService;
import eu.dissco.core.digitalspecimenprocessor.service.EntityRelationshipService;
import eu.dissco.core.digitalspecimenprocessor.service.EqualityService;
import eu.dissco.core.digitalspecimenprocessor.service.FdoRecordService;
import eu.dissco.core.digitalspecimenprocessor.service.MasSchedulerService;
import eu.dissco.core.digitalspecimenprocessor.service.RabbitMqPublisherService;
import eu.dissco.core.digitalspecimenprocessor.web.PidComponent;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;
import tools.jackson.databind.json.JsonMapper;

@Service
@Profile({ Profiles.WEB, Profiles.MEDIA_RABBIT_MQ })
@Slf4j
public class MediaPreprocessingService extends AbstractPreprocessingService {

	private final MasSchedulerService masSchedulerService;

	public MediaPreprocessingService(JsonMapper objectMapper, DigitalMediaRepository mediaRepository,
			DigitalMediaService digitalMediaService, RabbitMqPublisherService publisherService,
			EntityRelationshipService entityRelationshipService, EqualityService equalityService,
			ApplicationProperties applicationProperties, FdoRecordService fdoRecordService, PidComponent pidComponent,
			MasSchedulerService masSchedulerService) {
		super(objectMapper, mediaRepository, digitalMediaService, publisherService, entityRelationshipService,
				equalityService, applicationProperties, fdoRecordService, pidComponent);
		this.masSchedulerService = masSchedulerService;

	}

	public MediaProcessResult handleMessagesMedia(List<DigitalMediaEvent> events) {
		var uniqueBatchMedia = removeDuplicateMediaInBatch(events);
		var existingMedia = getCurrentMedia(uniqueBatchMedia);
		var mediaPids = processMediaPids(existingMedia, uniqueBatchMedia);
		var mediaProcessResult = preprocessMedia(uniqueBatchMedia, existingMedia, mediaPids);
		var mediaResult = processMedia(mediaProcessResult, mediaPids);
		masSchedulerService.scheduleMasForMedia(mediaResult);
		return mediaResult;
	}

	private Set<DigitalMediaEvent> removeDuplicateMediaInBatch(List<DigitalMediaEvent> mediaEvents) {
		var uniqueSet = new LinkedHashSet<DigitalMediaEvent>();
		var map = mediaEvents.stream()
			.collect(Collectors.groupingBy(event -> event.digitalMediaWrapper().attributes().getAcAccessURI(),
					Collectors.toCollection(LinkedHashSet::new)));
		for (var entry : map.entrySet()) {
			if (entry.getValue().size() > 1) {
				log.warn("Found {} duplicate media in batch for id {}", entry.getValue().size(), entry.getKey());
				boolean isFirst = true;
				for (var event : entry.getValue()) {
					if (isFirst) {
						uniqueSet.add(event);
						isFirst = false;
					}
					else {
						republishMediaEvent(event);
					}
				}
			}
			else {
				entry.getValue().stream().findFirst().ifPresent(uniqueSet::add);
			}
		}
		if (uniqueSet.size() > applicationProperties.getMaxMedia()) {
			log.error("Too many media in batch. Attempting to publish {} media at once", uniqueSet.size());
			throw new TooManyObjectsException("Attempting to publish too many media objects. Max is 10000");
		}
		return uniqueSet;
	}

	private void republishMediaEvent(DigitalMediaEvent event) {
		publisherService.republishMediaEvent(event);
	}

	private Map<String, PidProcessResult> processMediaPids(Map<String, DigitalMediaRecord> existingMedias,
			Set<DigitalMediaEvent> digitalMediaEvents) {
		var mediaPidMap = createPidsForNewMediaObjects(existingMedias, digitalMediaEvents).entrySet()
			.stream()
			.collect(Collectors.toMap(Entry::getKey, e -> new PidProcessResult(e.getValue(), Set.of())));
		mediaPidMap.putAll(existingMedias.entrySet()
			.stream()
			.collect(Collectors.toMap(Entry::getKey, e -> new PidProcessResult(e.getValue().id(), Set.of()))));
		return mediaPidMap;
	}

}
