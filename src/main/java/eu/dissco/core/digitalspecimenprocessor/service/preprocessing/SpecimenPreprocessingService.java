package eu.dissco.core.digitalspecimenprocessor.service.preprocessing;

import static java.util.stream.Collectors.toMap;

import eu.dissco.core.digitalspecimenprocessor.Profiles;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaEvent;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaRecord;
import eu.dissco.core.digitalspecimenprocessor.domain.media.MediaProcessResult;
import eu.dissco.core.digitalspecimenprocessor.domain.relation.MediaRelationshipProcessResult;
import eu.dissco.core.digitalspecimenprocessor.domain.relation.PidProcessResult;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenEvent;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenRecord;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.SpecimenPreprocessResult;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.SpecimenProcessResult;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.UpdatedDigitalSpecimenTuple;
import eu.dissco.core.digitalspecimenprocessor.exception.AnnotationProcessingException;
import eu.dissco.core.digitalspecimenprocessor.exception.DisscoRepositoryException;
import eu.dissco.core.digitalspecimenprocessor.exception.TooManyObjectsException;
import eu.dissco.core.digitalspecimenprocessor.property.ApplicationProperties;
import eu.dissco.core.digitalspecimenprocessor.repository.DigitalMediaRepository;
import eu.dissco.core.digitalspecimenprocessor.repository.DigitalSpecimenRepository;
import eu.dissco.core.digitalspecimenprocessor.service.AnnotationService;
import eu.dissco.core.digitalspecimenprocessor.service.DigitalMediaService;
import eu.dissco.core.digitalspecimenprocessor.service.DigitalSpecimenService;
import eu.dissco.core.digitalspecimenprocessor.service.EntityRelationshipService;
import eu.dissco.core.digitalspecimenprocessor.service.EqualityService;
import eu.dissco.core.digitalspecimenprocessor.service.FdoRecordService;
import eu.dissco.core.digitalspecimenprocessor.service.MasSchedulerService;
import eu.dissco.core.digitalspecimenprocessor.service.RabbitMqPublisherService;
import eu.dissco.core.digitalspecimenprocessor.web.PidComponent;
import io.github.dissco.core.annotationlogic.schema.Annotation;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;
import tools.jackson.databind.json.JsonMapper;

@Service
@Profile({ Profiles.WEB, Profiles.SPECIMEN_RABBIT_MQ })
@Slf4j
public class SpecimenPreprocessingService extends AbstractPreprocessingService {

	private final MasSchedulerService masSchedulerService;

	private final DigitalSpecimenRepository specimenRepository;

	private final AnnotationService annotationService;

	private final DigitalSpecimenService digitalSpecimenService;

	public SpecimenPreprocessingService(JsonMapper objectMapper, DigitalMediaRepository mediaRepository,
			DigitalMediaService digitalMediaService, RabbitMqPublisherService publisherService,
			EntityRelationshipService entityRelationshipService, EqualityService equalityService,
			ApplicationProperties applicationProperties, FdoRecordService fdoRecordService, PidComponent pidComponent,
			MasSchedulerService masSchedulerService, DigitalSpecimenRepository specimenRepository,
			AnnotationService annotationService, DigitalSpecimenService digitalSpecimenService) {
		super(objectMapper, mediaRepository, digitalMediaService, publisherService, entityRelationshipService,
				equalityService, applicationProperties, fdoRecordService, pidComponent);
		this.masSchedulerService = masSchedulerService;
		this.specimenRepository = specimenRepository;
		this.annotationService = annotationService;
		this.digitalSpecimenService = digitalSpecimenService;
	}

	public SpecimenProcessResult handleMessages(List<DigitalSpecimenEvent> events) {
		log.info("Processing {} digital specimen", events.size());
		try {
			var validEvents = checkImageDuplicationSingleSpecimen(events);
			var uniqueBatchSpecimens = removeDuplicatesInBatch(validEvents);
			if (uniqueBatchSpecimens.isEmpty()) {
				log.warn("After removing any non-complaint events, there is nothing to process");
				return new SpecimenProcessResult();
			}
			var uniqueBatchMedia = getUniqueDigitalMediaEvents(uniqueBatchSpecimens);
			var existingSpecimens = getCurrentSpecimen(uniqueBatchSpecimens);
			var existingMedia = getCurrentMedia(uniqueBatchMedia);
			var annotationsForSpecimens = getAcceptedAnnotationsForSpecimens(existingSpecimens);
			log.info("Retrieved {} existing specimen, {} existing media", existingSpecimens.size(),
					existingMedia.size());
			var specimenPreprocessResult = preprocessSpecimens(uniqueBatchSpecimens, existingSpecimens, existingMedia,
					annotationsForSpecimens);
			var pids = processPids(specimenPreprocessResult, existingMedia, uniqueBatchSpecimens, uniqueBatchMedia);
			var mediaPreprocessResult = preprocessMedia(uniqueBatchMedia, existingMedia, pids.getRight());
			log.info("Batch consists of: {} new, {} update, and {} equal specimens",
					specimenPreprocessResult.newSpecimens().size(), specimenPreprocessResult.changedSpecimens().size(),
					specimenPreprocessResult.equalSpecimens().size());
			log.info("Batch consists of {} new, {} update, and {} equal media",
					mediaPreprocessResult.newDigitalMedia().size(), mediaPreprocessResult.changedDigitalMedia().size(),
					mediaPreprocessResult.equalDigitalMedia().size());
			var specimenResults = processSpecimens(specimenPreprocessResult, pids.getLeft());
			var mediaPids = updateMediaPidsWithResults(specimenResults, specimenPreprocessResult, pids.getRight());
			var mediaResults = processMedia(mediaPreprocessResult, mediaPids);
			log.info("Processed specimen and media");
			scheduleMas(specimenResults, mediaResults);
			return specimenResults;
		}
		catch (DisscoRepositoryException e) {
			log.error("Unable to access database", e);
			return new SpecimenProcessResult(Map.of(), List.of(), List.of());
		}
	}

	private void scheduleMas(SpecimenProcessResult specimenResult, MediaProcessResult mediaProcessResult) {
		masSchedulerService.scheduleMasForSpecimen(specimenResult);
		masSchedulerService.scheduleMasForMedia(mediaProcessResult);
	}

	private List<DigitalSpecimenEvent> checkImageDuplicationSingleSpecimen(List<DigitalSpecimenEvent> events) {
		var validEvent = new ArrayList<DigitalSpecimenEvent>();
		for (var event : events) {
			if (event.digitalMediaEvents()
				.stream()
				.collect(Collectors.groupingBy(e -> e.digitalMediaWrapper().attributes().getAcAccessURI()))
				.values()
				.stream()
				.anyMatch(e -> e.size() > 1)) {
				log.error("Found duplicate media in single specimen event for specimen id: {}. Dead lettering event",
						event.digitalSpecimenWrapper().physicalSpecimenID());
				publisherService.deadLetterEventSpecimen(event);
			}
			else {
				validEvent.add(event);
			}
		}
		return validEvent;
	}

	private Set<DigitalSpecimenEvent> removeDuplicatesInBatch(List<DigitalSpecimenEvent> events) {
		var uniqueSet = new LinkedHashSet<DigitalSpecimenEvent>();
		var uniqueMediaSet = new LinkedHashSet<String>();
		var map = events.stream()
			.collect(Collectors.groupingBy(event -> event.digitalSpecimenWrapper().physicalSpecimenID()));
		for (var entry : map.entrySet()) {
			if (entry.getValue().size() > 1) {
				log.warn("Found {} duplicate specimen in batch for id {}", entry.getValue().size(), entry.getKey());
				var specimenIsNotPublished = true;
				for (var duplicateSpecimenEvent : entry.getValue()) {
					if (specimenIsNotPublished && checkIfMediaIsUnique(duplicateSpecimenEvent, uniqueMediaSet)) {
						addToUniqueSets(uniqueSet, duplicateSpecimenEvent, uniqueMediaSet);
						specimenIsNotPublished = false;
					}
					else {
						republishSpecimenEvent(duplicateSpecimenEvent);
					}
				}
			}
			else if (checkIfMediaIsUnique(entry.getValue().getFirst(), uniqueMediaSet)) {
				addToUniqueSets(uniqueSet, entry.getValue().getFirst(), uniqueMediaSet);
			}
			else {
				republishSpecimenEvent(entry.getValue().getFirst());
			}
		}
		return uniqueSet;
	}

	private static boolean checkIfMediaIsUnique(DigitalSpecimenEvent entry, HashSet<String> uniqueMediaSet) {
		return entry.digitalMediaEvents()
			.stream()
			.map(e -> e.digitalMediaWrapper().attributes().getAcAccessURI())
			.noneMatch(uniqueMediaSet::contains);
	}

	private static void addToUniqueSets(LinkedHashSet<DigitalSpecimenEvent> uniqueSet, DigitalSpecimenEvent entry,
			HashSet<String> uniqueMediaSet) {
		uniqueSet.add(entry);
		uniqueMediaSet.addAll(entry.digitalMediaEvents()
			.stream()
			.map(e -> e.digitalMediaWrapper().attributes().getAcAccessURI())
			.toList());
	}

	private void republishSpecimenEvent(DigitalSpecimenEvent event) {
		publisherService.republishSpecimenEvent(event);
	}

	private Set<DigitalMediaEvent> getUniqueDigitalMediaEvents(Set<DigitalSpecimenEvent> validEvents) {
		var uniqueBatchMedia = validEvents.stream()
			.flatMap(event -> event.digitalMediaEvents().stream())
			.collect(Collectors.toSet());
		if (uniqueBatchMedia.size() > applicationProperties.getMaxMedia()) {
			log.error("Too many media in batch. Attempting to publish {} media at once", uniqueBatchMedia.size());
			throw new TooManyObjectsException("Attempting to publish too many media objects. Max is 10000");
		}
		return uniqueBatchMedia;
	}

	private Map<String, DigitalSpecimenRecord> getCurrentSpecimen(Set<DigitalSpecimenEvent> events)
			throws DisscoRepositoryException {
		var eventMap = events.stream()
			.collect(Collectors.toMap(event -> event.digitalSpecimenWrapper().physicalSpecimenID(),
					Function.identity()));
		return specimenRepository
			.getDigitalSpecimens(
					events.stream().map(event -> event.digitalSpecimenWrapper().physicalSpecimenID()).toList())
			.stream()
			.map(dbRecord -> {
				var event = eventMap.get(dbRecord.digitalSpecimenWrapper().physicalSpecimenID());
				return new DigitalSpecimenRecord(dbRecord.id(), dbRecord.midsLevel(), dbRecord.version(),
						dbRecord.created(), dbRecord.digitalSpecimenWrapper(), event.masList(),
						event.forceMasSchedule(), event.isDataFromSourceSystem(), List.of());
			})
			.collect(toMap(specimenRecord -> specimenRecord.digitalSpecimenWrapper().physicalSpecimenID(),
					Function.identity()));
	}

	private Map<String, List<Annotation>> getAcceptedAnnotationsForSpecimens(
			Map<String, DigitalSpecimenRecord> digitalSpecimenRecords) {
		return annotationService.getAnnotationsForSpecimens(new HashSet<>(digitalSpecimenRecords.values()));
	}

	private SpecimenPreprocessResult preprocessSpecimens(Set<DigitalSpecimenEvent> events,
			Map<String, DigitalSpecimenRecord> currentSpecimens, Map<String, DigitalMediaRecord> currentMedia,
			Map<String, List<Annotation>> acceptedAnnotations) {
		var equalSpecimens = new HashMap<DigitalSpecimenRecord, DigitalSpecimenEvent>();
		var changedSpecimens = new ArrayList<UpdatedDigitalSpecimenTuple>();
		var newSpecimens = new ArrayList<DigitalSpecimenEvent>();
		for (DigitalSpecimenEvent event : events) {
			log.debug("ds: {}", event.digitalSpecimenWrapper());
			if (!currentSpecimens.containsKey(event.digitalSpecimenWrapper().physicalSpecimenID())) {
				log.debug("Specimen with id: {} is completely new",
						event.digitalSpecimenWrapper().physicalSpecimenID());
				newSpecimens.add(event);
			}
			else {
				var currentDigitalSpecimen = currentSpecimens.get(event.digitalSpecimenWrapper().physicalSpecimenID());
				var processedMediaRelationships = getMediaRelationships(event, currentSpecimens, currentMedia);
				event = applyAcceptedAnnotationsToEvent(event, currentDigitalSpecimen, acceptedAnnotations);
				if (event != null) {
					if (equalityService.specimensAreEqual(currentDigitalSpecimen, event.digitalSpecimenWrapper(),
							processedMediaRelationships)) {
						log.debug("Received digital specimen is equal to digital specimen: {}",
								currentDigitalSpecimen.id());
						equalSpecimens.put(currentDigitalSpecimen, event);
					}
					else {
						log.debug("Specimen with id: {} has received an update", currentDigitalSpecimen.id());
						var eventWithUpdatedEr = equalityService.setExistingEventDatesSpecimen(
								currentDigitalSpecimen.digitalSpecimenWrapper(), event, processedMediaRelationships);
						changedSpecimens.add(new UpdatedDigitalSpecimenTuple(currentDigitalSpecimen, eventWithUpdatedEr,
								processedMediaRelationships));
					}
				}
			}
		}
		var newSpecimenPids = createNewSpecimenPids(newSpecimens);
		return new SpecimenPreprocessResult(equalSpecimens, changedSpecimens, newSpecimens, newSpecimenPids);
	}

	private MediaRelationshipProcessResult getMediaRelationships(DigitalSpecimenEvent event,
			Map<String, DigitalSpecimenRecord> currentSpecimens, Map<String, DigitalMediaRecord> currentMedia) {
		MediaRelationshipProcessResult processedMediaRelationships;
		if (Boolean.TRUE.equals(event.isDataFromSourceSystem())) {
			processedMediaRelationships = entityRelationshipService
				.processMediaRelationshipsForSpecimen(currentSpecimens, event, currentMedia);
		}
		else {
			processedMediaRelationships = new MediaRelationshipProcessResult();
		}
		return processedMediaRelationships;
	}

	private DigitalSpecimenEvent applyAcceptedAnnotationsToEvent(DigitalSpecimenEvent event,
			DigitalSpecimenRecord currentDigitalSpecimen, Map<String, List<Annotation>> acceptedAnnotations) {
		try {
			return annotationService.applyAcceptedAnnotations(event, currentDigitalSpecimen, acceptedAnnotations);
		}
		catch (AnnotationProcessingException _) {
			log.info("Failed to apply annotations to specimen {}. Moving to DLQ", currentDigitalSpecimen.id());
			publisherService.deadLetterEventSpecimen(event);
			return null;
		}
	}

	private SpecimenProcessResult processSpecimens(SpecimenPreprocessResult specimenPreprocessResult,
			Map<String, PidProcessResult> pidProcessResults) {
		var equalSpecimens = new HashMap<DigitalSpecimenRecord, DigitalSpecimenEvent>();
		var updatedSpecimens = new ArrayList<DigitalSpecimenRecord>();
		var newSpecimens = new ArrayList<DigitalSpecimenRecord>();
		if (!specimenPreprocessResult.equalSpecimens().isEmpty()) {
			digitalSpecimenService.updateEqualSpecimen(specimenPreprocessResult.equalSpecimens());
			equalSpecimens = new HashMap<>(specimenPreprocessResult.equalSpecimens());
		}
		if (!specimenPreprocessResult.newSpecimens().isEmpty()) {
			if (!specimenPreprocessResult.newSpecimenPids().isEmpty()) {
				newSpecimens = new ArrayList<>(digitalSpecimenService
					.createNewDigitalSpecimen(specimenPreprocessResult.newSpecimens(), pidProcessResults));
			}
			else {
				log.warn("Unable to create new specimen pids for {} specimens. Ignoring new specimens",
						specimenPreprocessResult.newSpecimens().size());
			}
		}
		if (!specimenPreprocessResult.changedSpecimens().isEmpty()) {
			updatedSpecimens = new ArrayList<>(digitalSpecimenService
				.updateExistingDigitalSpecimen(specimenPreprocessResult.changedSpecimens(), pidProcessResults));
		}
		return new SpecimenProcessResult(equalSpecimens, updatedSpecimens, newSpecimens);
	}

	private Map<String, String> createNewSpecimenPids(List<DigitalSpecimenEvent> events) {
		if (events.isEmpty()) {
			return Map.of();
		}
		var specimenList = events.stream().map(DigitalSpecimenEvent::digitalSpecimenWrapper).toList();
		var pidMap = new HashMap<String, String>();
		for (int i = 0; i < specimenList.size(); i += applicationProperties.getMaxPids()) {
			int j = Math.min(i + applicationProperties.getMaxPids(), specimenList.size());
			var sublist = specimenList.subList(i, j);
			var request = fdoRecordService.buildPostPidRequest(sublist);
			pidMap.putAll(createNewPids(request, true));
		}
		return pidMap;
	}

	/*
	 * We need a way to map the specimen PIDs to the media and vice versa Each has a
	 * many-to-many relationship
	 */
	private Pair<Map<String, PidProcessResult>, Map<String, PidProcessResult>> processPids(
			SpecimenPreprocessResult specimenPreprocessResult, Map<String, DigitalMediaRecord> existingMedias,
			Set<DigitalSpecimenEvent> digitalSpecimenEvents, Set<DigitalMediaEvent> digitalMediaEvents) {
		var specimenPids = new HashMap<String, PidProcessResult>();
		var mediaHashMap = new HashMap<String, HashSet<String>>(); // key = local id
		var allSpecimenPids = concatSpecimenPids(specimenPreprocessResult);
		var newMediaPids = createPidsForNewMediaObjects(existingMedias, digitalMediaEvents); // key
		// =
		// local
		// id
		var allMediaPids = concatMediaPids(existingMedias, newMediaPids); // key = local
		// id
		for (var specimen : digitalSpecimenEvents) {
			var mediaDoisForThisSpecimen = specimen.digitalMediaEvents()
				.stream()
				.map(event -> event.digitalMediaWrapper().attributes().getAcAccessURI())
				.map(allMediaPids::get)
				.collect(Collectors.toSet());
			var specimenPid = allSpecimenPids.get(specimen.digitalSpecimenWrapper().physicalSpecimenID());
			specimenPids.put(specimen.digitalSpecimenWrapper().physicalSpecimenID(),
					new PidProcessResult(specimenPid, mediaDoisForThisSpecimen));
			// We record the specimen -> media relationship in an intermediate map
			// Multiple specimens may refer to the same media object
			// But the media event doesn't have a direct link to the specimens it refers
			// to
			updateMediaHashMap(mediaHashMap, mediaDoisForThisSpecimen, allMediaPids, specimenPid);
		}
		var mediaPids = mediaHashMap.entrySet()
			.stream()
			.collect(toMap(Entry::getKey, e -> new PidProcessResult(allMediaPids.get(e.getKey()), e.getValue())));
		for (var mediaPid : mediaPids.entrySet()) {
			if (mediaPid.getValue().doisOfRelatedObjects().size() > 1) {
				log.info("Media {} has {} related specimens", mediaPid.getValue().doiOfTarget(),
						mediaPid.getValue().doisOfRelatedObjects().size());
			}
		}
		return Pair.of(specimenPids, mediaPids);
	}

	private static Map<String, String> concatSpecimenPids(SpecimenPreprocessResult specimenPreprocessResult) {
		var existingSpecimenPids = Stream.concat(specimenPreprocessResult.equalSpecimens().keySet().stream(),
				specimenPreprocessResult.changedSpecimens().stream().map(UpdatedDigitalSpecimenTuple::currentSpecimen))
			.collect(toMap(specimen -> specimen.digitalSpecimenWrapper().physicalSpecimenID(),
					DigitalSpecimenRecord::id));
		return concatMaps(specimenPreprocessResult.newSpecimenPids(), existingSpecimenPids);
	}

	private static Map<String, String> concatMediaPids(Map<String, DigitalMediaRecord> existingMedias,
			Map<String, String> newMediaPids) {
		var existingPidMap = existingMedias.entrySet().stream().collect(toMap(Entry::getKey, e -> e.getValue().id()));
		return concatMaps(existingPidMap, newMediaPids);
	}

	private static Map<String, String> concatMaps(Map<String, String> m1, Map<String, String> m2) {
		return Stream.concat(m1.entrySet().stream(), m2.entrySet().stream())
			.collect(toMap(Entry::getKey, Entry::getValue));
	}

	private static void updateMediaHashMap(HashMap<String, HashSet<String>> mediaHashMap,
			Set<String> mediaPidsForThisSpecimen, Map<String, String> allMediaPids, String specimenPid) {
		if (mediaPidsForThisSpecimen.isEmpty()) {
			return;
		}
		allMediaPids.entrySet()
			.stream()
			.filter(e -> mediaPidsForThisSpecimen.contains(e.getValue())) // Only look at
			// relevant
			// pids
			.forEach(e -> {
				var uri = e.getKey();
				mediaHashMap.computeIfAbsent(uri, k -> new HashSet<>()).add(specimenPid);
			});
	}

	private static Map<String, PidProcessResult> updateMediaPidsWithResults(SpecimenProcessResult specimenResult,
			SpecimenPreprocessResult specimenPreprocessResult, Map<String, PidProcessResult> mediaPidsFull) {
		if ((specimenResult.updatedDigitalSpecimens().size()
				+ specimenPreprocessResult.newSpecimens().size()) < (specimenPreprocessResult.changedSpecimens().size()
						+ specimenPreprocessResult.newSpecimens().size())) {
			return mediaPidsFull;
		}
		// If we had a partial success, and not all specimens were created, we don't want
		// to create meaningless ERS on our media
		// So we filter out the specimen PIDs that were not in our results
		var changedSpecimens = Stream
			.concat(specimenResult.updatedDigitalSpecimens().stream(), specimenResult.newDigitalSpecimens().stream())
			.toList();
		var specimenDOIs = changedSpecimens.stream().map(DigitalSpecimenRecord::id).toList();
		var mediaPidsFiltered = new HashMap<String, PidProcessResult>();
		for (var mediaPid : mediaPidsFull.entrySet()) {
			var relatedDois = mediaPid.getValue()
				.doisOfRelatedObjects()
				.stream()
				.filter(specimenDOIs::contains)
				.collect(Collectors.toSet());
			mediaPidsFiltered.put(mediaPid.getKey(),
					new PidProcessResult(mediaPid.getValue().doiOfTarget(), relatedDois));
		}
		return mediaPidsFiltered;
	}

}
