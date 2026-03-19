package eu.dissco.core.digitalspecimenprocessor.service;

import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaEvent;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaRecord;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaWrapper;
import eu.dissco.core.digitalspecimenprocessor.domain.media.UpdatedDigitalMediaRecord;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenEvent;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenRecord;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.UpdatedDigitalSpecimenRecord;
import eu.dissco.core.digitalspecimenprocessor.exception.PidException;
import eu.dissco.core.digitalspecimenprocessor.repository.DigitalMediaRepository;
import eu.dissco.core.digitalspecimenprocessor.repository.DigitalSpecimenRepository;
import eu.dissco.core.digitalspecimenprocessor.repository.ElasticSearchRepository;
import eu.dissco.core.digitalspecimenprocessor.web.PidComponent;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jooq.exception.DataAccessException;
import org.springframework.stereotype.Service;
import tools.jackson.databind.JsonNode;

@Service
@RequiredArgsConstructor
@Slf4j
public class RollbackService {

	private final ElasticSearchRepository elasticRepository;

	private final RabbitMqPublisherService publisherService;

	private final DigitalSpecimenRepository specimenRepository;

	private final DigitalMediaRepository mediaRepository;

	private final FdoRecordService fdoRecordService;

	private final PidComponent pidComponent;

	// Rollback updated specimen

	public void rollbackUpdatedSpecimens(Set<UpdatedDigitalSpecimenRecord> updatedDigitalSpecimenRecords,
			boolean elasticRollback, boolean databseRollback) {
		// Rollback in database and/or in elastic
		updatedDigitalSpecimenRecords
			.forEach(updatedRecord -> rollbackUpdatedSpecimen(updatedRecord, elasticRollback, databseRollback));
		// Rollback PID records for those that need it
		filterUpdatesAndRollbackPidsSpecimen(updatedDigitalSpecimenRecords);
	}

	private void rollbackUpdatedSpecimen(UpdatedDigitalSpecimenRecord updatedDigitalSpecimenRecord,
			boolean elasticRollback, boolean databaseRollback) {
		if (elasticRollback) {
			try {
				elasticRepository.rollbackVersion(updatedDigitalSpecimenRecord.currentDigitalSpecimen());
			}
			catch (IOException | ElasticsearchException e) {
				log.error("Fatal exception, unable to roll back update for: {}",
						updatedDigitalSpecimenRecord.currentDigitalSpecimen().id(), e);
			}
		}
		if (databaseRollback) {
			rollBackToEarlierDatabaseVersionSpecimen(updatedDigitalSpecimenRecord.currentDigitalSpecimen());
		}
		publisherService
			.deadLetterEventSpecimen(specimenEventFromRecord(updatedDigitalSpecimenRecord.digitalSpecimenRecord()));
	}

	private void rollBackToEarlierDatabaseVersionSpecimen(DigitalSpecimenRecord currentDigitalSpecimen) {
		try {
			specimenRepository.updateDigitalSpecimenRecord(Set.of(currentDigitalSpecimen));
		}
		catch (DataAccessException _) {
			log.error("Unable to rollback specimen {} to previous version", currentDigitalSpecimen.id());
		}
	}

	private void rollBackToEarlierDatabaseVersionMedia(DigitalMediaRecord currentDigitalMedia) {
		try {
			mediaRepository.updateDigitalMediaRecord(Set.of(currentDigitalMedia));
		}
		catch (DataAccessException _) {
			log.error("Unable to rollback media {} to previous version", currentDigitalMedia.id());
		}
	}

	// Rollback Updated Media

	public void rollbackUpdatedMedias(Set<UpdatedDigitalMediaRecord> updatedDigitalMediaRecords,
			boolean elasticRollback, boolean databaseRollback) {
		updatedDigitalMediaRecords
			.forEach(updatedRecord -> rollbackUpdatedMedia(updatedRecord, elasticRollback, databaseRollback));
		// Rollback PID records for those that need it
		filterUpdatesAndRollbackPidsMedia(updatedDigitalMediaRecords);
	}

	private void rollbackUpdatedMedia(UpdatedDigitalMediaRecord updatedDigitalMediaRecord, boolean elasticRollback,
			boolean databaseRollback) {
		if (elasticRollback) {
			try {
				elasticRepository.rollbackVersion(updatedDigitalMediaRecord.currentDigitalMediaRecord());
			}
			catch (IOException | ElasticsearchException e) {
				log.error("Fatal exception, unable to roll back update for: "
						+ updatedDigitalMediaRecord.currentDigitalMediaRecord(), e);
			}
		}
		if (databaseRollback) {
			rollBackToEarlierDatabaseVersionMedia(updatedDigitalMediaRecord.currentDigitalMediaRecord());
		}
		publisherService.deadLetterEventMedia(mediaEventFromRecord(updatedDigitalMediaRecord.digitalMediaRecord()));
	}

	// Rollback New Specimen
	public void rollbackNewSpecimens(Set<DigitalSpecimenRecord> digitalSpecimenRecords, boolean elasticRollback,
			boolean databaseRollback) {
		// Rollback in database and/or elastic
		digitalSpecimenRecords.forEach(
				digitalSpecimenRecord -> rollbackNewSpecimen(digitalSpecimenRecord, elasticRollback, databaseRollback));
		// Rollback PID creation for specimen
	}

	private void rollbackNewSpecimen(DigitalSpecimenRecord digitalSpecimenRecord, boolean elasticRollback,
			boolean databaseRollback) {
		if (elasticRollback) {
			try {
				elasticRepository.rollbackObject(digitalSpecimenRecord.id(), true);
			}
			catch (IOException | ElasticsearchException e) {
				log.error("Fatal exception, unable to roll back: {}", digitalSpecimenRecord.id(), e);
			}
		}
		if (databaseRollback) {
			specimenRepository.rollbackSpecimen(digitalSpecimenRecord.id());
		}
		publisherService.deadLetterEventSpecimen(specimenEventFromRecord(digitalSpecimenRecord));
	}

	// Rollback New Media
	public void rollbackNewMedias(Set<DigitalMediaRecord> digitalMediaRecords, boolean elasticRollback,
			boolean databaseRollback) {
		// Rollback in database and/or elastic
		digitalMediaRecords
			.forEach(digitalMediaRecord -> rollbackNewMedia(digitalMediaRecord, elasticRollback, databaseRollback));
	}

	private void rollbackNewMedia(DigitalMediaRecord digitalMediaRecord, boolean elasticRollback,
			boolean databaseRollback) {
		if (elasticRollback) {
			try {
				elasticRepository.rollbackObject(digitalMediaRecord.id(), false);
			}
			catch (IOException | ElasticsearchException e) {
				log.error("Fatal exception, unable to roll back: {}", digitalMediaRecord.id(), e);
			}
		}
		if (databaseRollback) {
			mediaRepository.rollBackDigitalMedia(digitalMediaRecord.id());
		}
		publisherService.deadLetterEventMedia(mediaEventFromRecord(digitalMediaRecord));
	}

	private static DigitalMediaEvent mediaEventFromRecord(DigitalMediaRecord digitalMediaRecord) {
		return new DigitalMediaEvent(digitalMediaRecord.masIds(),
				new DigitalMediaWrapper(digitalMediaRecord.attributes().getOdsFdoType(),
						digitalMediaRecord.attributes(), digitalMediaRecord.originalAttributes()),
				digitalMediaRecord.forceMasSchedule(), digitalMediaRecord.isDataFromSourceSystem());
	}

	private static DigitalSpecimenEvent specimenEventFromRecord(DigitalSpecimenRecord digitalSpecimenRecord) {
		return new DigitalSpecimenEvent(digitalSpecimenRecord.masIds(), digitalSpecimenRecord.digitalSpecimenWrapper(),
				digitalSpecimenRecord.digitalMediaEvents(), digitalSpecimenRecord.forceMasSchedule(),
				digitalSpecimenRecord.isDataFromSourceSystem());
	}

	// Elastic Failures
	public Set<DigitalSpecimenRecord> handlePartiallyFailedElasticInsertSpecimen(
			Set<DigitalSpecimenRecord> digitalSpecimenRecords, BulkResponse bulkResponse) {
		var digitalSpecimenMap = digitalSpecimenRecords.stream()
			.collect(Collectors.toMap(DigitalSpecimenRecord::id, Function.identity()));
		var rollbackDigitalRecordIds = new HashSet<String>();
		bulkResponse.items().forEach(item -> {
			var digitalSpecimenRecord = digitalSpecimenMap.get(item.id());
			if (item.error() != null) {
				log.error("Failed to insert item into elastic search: {} with errors {}", item.id(),
						item.error().reason());
				rollbackDigitalRecordIds.add(item.id());
				rollbackNewSpecimen(digitalSpecimenRecord, false, true);
			}
			else {
				publishCreateEventSpecimen(digitalSpecimenRecord);
			}
		});
		return new HashSet<>(digitalSpecimenRecords).stream()
			.filter(r -> !rollbackDigitalRecordIds.contains(r.id()))
			.collect(Collectors.toSet());
	}

	public Set<DigitalMediaRecord> handlePartiallyFailedElasticInsertMedia(Set<DigitalMediaRecord> digitalMediaRecords,
			BulkResponse bulkResponse) {
		var digitalMediaRecordMap = digitalMediaRecords.stream()
			.collect(Collectors.toMap(DigitalMediaRecord::id, Function.identity()));
		var rollbackDigitalRecordIds = new HashSet<String>();
		bulkResponse.items().forEach(item -> {
			var digitalMediaRecord = digitalMediaRecordMap.get(item.id());
			if (item.error() != null) {
				log.error("Failed to insert item into elastic search: {} with errors {}", item.id(),
						item.error().reason());
				rollbackDigitalRecordIds.add(item.id());
				rollbackNewMedia(digitalMediaRecord, false, true);
			}
			else {
				publishCreateEventMedia(digitalMediaRecord);
			}
		});
		return digitalMediaRecords.stream()
			.filter(digitalMediaRecord -> !rollbackDigitalRecordIds.contains(digitalMediaRecord.id()))
			.collect(Collectors.toSet());
	}

	public Set<UpdatedDigitalSpecimenRecord> handlePartiallyFailedElasticUpdateSpecimen(
			Set<UpdatedDigitalSpecimenRecord> digitalSpecimenRecords, BulkResponse bulkResponse) {
		var digitalSpecimenMap = digitalSpecimenRecords.stream()
			.collect(Collectors.toMap(
					updatedDigitalSpecimenRecord -> updatedDigitalSpecimenRecord.digitalSpecimenRecord().id(),
					Function.identity()));
		var mutableDigitalSpecimenRecords = new HashSet<>(digitalSpecimenRecords);
		var pidUpdatesToRollback = new ArrayList<UpdatedDigitalSpecimenRecord>();
		bulkResponse.items().forEach(item -> {
			var digitalSpecimenRecord = digitalSpecimenMap.get(item.id());
			if (item.error() != null) {
				log.error("Failed to update item into elastic search: {} with errors {}",
						digitalSpecimenRecord.digitalSpecimenRecord().id(), item.error().reason());
				pidUpdatesToRollback.add(digitalSpecimenRecord);
				rollbackUpdatedSpecimen(digitalSpecimenRecord, false, true);
				mutableDigitalSpecimenRecords.remove(digitalSpecimenRecord);
			}
			else {
				publishUpdateEventSpecimen(digitalSpecimenRecord);
			}
		});
		filterUpdatesAndRollbackPidsSpecimen(pidUpdatesToRollback);
		return mutableDigitalSpecimenRecords;
	}

	public Set<UpdatedDigitalMediaRecord> handlePartiallyFailedElasticUpdateMedia(
			Set<UpdatedDigitalMediaRecord> digitalMediaRecords, BulkResponse bulkResponse) {
		var digitalMediaMap = digitalMediaRecords.stream()
			.collect(Collectors.toMap(updatedDigitalMediaRecord -> updatedDigitalMediaRecord.digitalMediaRecord().id(),
					Function.identity()));
		var digitalMediaRecordsMutable = new HashSet<>(digitalMediaRecords);
		List<UpdatedDigitalMediaRecord> pidsToRollback = new ArrayList<>();
		bulkResponse.items().forEach(item -> {
			var digitalMediaRecord = digitalMediaMap.get(item.id());
			if (item.error() != null) {
				log.error("Failed item to insert into elastic search: {} with errors {}",
						digitalMediaRecord.digitalMediaRecord().id(), item.error().reason());
				rollbackUpdatedMedia(digitalMediaRecord, false, true);
				pidsToRollback.add(digitalMediaRecord);
				digitalMediaRecordsMutable.remove(digitalMediaRecord);
			}
			else {
				publishUpdateEventMedia(digitalMediaRecord);
			}
		});
		filterUpdatesAndRollbackPidsMedia(pidsToRollback);
		return digitalMediaRecordsMutable;
	}

	// Event publishing
	private void publishUpdateEventSpecimen(UpdatedDigitalSpecimenRecord updatedDigitalSpecimenRecord) {
		publisherService.publishUpdateEventSpecimen(updatedDigitalSpecimenRecord.digitalSpecimenRecord(),
				updatedDigitalSpecimenRecord.jsonPatch());
	}

	private void publishUpdateEventMedia(UpdatedDigitalMediaRecord updatedDigitalMediaRecord) {
		publisherService.publishUpdateEventMedia(updatedDigitalMediaRecord.digitalMediaRecord(),
				updatedDigitalMediaRecord.jsonPatch());
	}

	private void publishCreateEventSpecimen(DigitalSpecimenRecord digitalSpecimenRecord) {
		publisherService.publishCreateEventSpecimen(digitalSpecimenRecord);
	}

	private void publishCreateEventMedia(DigitalMediaRecord digitalMediaRecord) {
		publisherService.publishCreateEventMedia(digitalMediaRecord);
	}

	// Only rollback the PID updates for records that were updated
	private void filterUpdatesAndRollbackPidsSpecimen(Collection<UpdatedDigitalSpecimenRecord> records) {
		var recordsToRollback = records.stream()
			.filter(r -> fdoRecordService.pidNeedsUpdateSpecimen(r.currentDigitalSpecimen().digitalSpecimenWrapper(),
					r.digitalSpecimenRecord().digitalSpecimenWrapper()))
			.map(UpdatedDigitalSpecimenRecord::currentDigitalSpecimen)
			.toList();
		var fdoRequest = fdoRecordService.buildRollbackUpdateRequest(recordsToRollback);
		rollbackPidUpdate(recordsToRollback.stream().map(DigitalSpecimenRecord::id).toList(), fdoRequest);
	}

	private void filterUpdatesAndRollbackPidsMedia(Collection<UpdatedDigitalMediaRecord> records) {
		var recordsToRollback = records.stream()
			.filter(r -> fdoRecordService.pidNeedsUpdateMedia(r.currentDigitalMediaRecord().attributes(),
					r.digitalMediaRecord().attributes()))
			.map(UpdatedDigitalMediaRecord::digitalMediaRecord)
			.toList();
		var fdoRequest = fdoRecordService.buildRollbackUpdateRequestMedia(recordsToRollback);
		rollbackPidUpdate(recordsToRollback.stream().map(DigitalMediaRecord::id).toList(), fdoRequest);
	}

	private void rollbackPidUpdate(List<String> ids, List<JsonNode> fdoRequest) {
		if (ids.isEmpty()) {
			return;
		}
		try {
			pidComponent.rollbackPidUpdate(fdoRequest);
		}
		catch (PidException _) {
			log.error("Unable to rollback PIDs for updated specimens. PIDs: {}", ids);
		}
	}

}
