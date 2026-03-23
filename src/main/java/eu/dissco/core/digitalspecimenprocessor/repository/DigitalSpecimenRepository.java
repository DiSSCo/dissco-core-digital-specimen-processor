package eu.dissco.core.digitalspecimenprocessor.repository;

import static eu.dissco.core.digitalspecimenprocessor.database.jooq.Tables.DIGITAL_SPECIMEN;

import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenEvent;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenRecord;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenWrapper;
import eu.dissco.core.digitalspecimenprocessor.exception.DisscoRepositoryException;
import eu.dissco.core.digitalspecimenprocessor.schema.DigitalSpecimen;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jooq.DSLContext;
import org.jooq.JSONB;
import org.jooq.Query;
import org.jooq.Record;
import org.jooq.exception.DataAccessException;
import org.springframework.stereotype.Repository;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

@Slf4j
@Repository
@RequiredArgsConstructor
public class DigitalSpecimenRepository {

	private final DSLContext context;

	private final JsonMapper mapper;

	private DigitalSpecimenRecord mapToDigitalSpecimenRecord(Record dbRecord) {
		var digitalSpecimenWrapper = new DigitalSpecimenWrapper(dbRecord.get(DIGITAL_SPECIMEN.PHYSICAL_SPECIMEN_ID),
				dbRecord.get(DIGITAL_SPECIMEN.TYPE), mapToDigitalSpecimen(dbRecord.get(DIGITAL_SPECIMEN.DATA)),
				mapToJson(dbRecord.get(DIGITAL_SPECIMEN.ORIGINAL_DATA)));
		return new DigitalSpecimenRecord(dbRecord.get(DIGITAL_SPECIMEN.ID), dbRecord.get(DIGITAL_SPECIMEN.MIDSLEVEL),
				dbRecord.get(DIGITAL_SPECIMEN.VERSION), dbRecord.get(DIGITAL_SPECIMEN.CREATED), digitalSpecimenWrapper,
				null, null, null, List.of());
	}

	private DigitalSpecimen mapToDigitalSpecimen(JSONB jsonb) {
		try {
			return mapper.readValue(jsonb.data(), DigitalSpecimen.class);
		}
		catch (JacksonException e) {
			log.warn("Unable to map jsonb to digital specimen: {}", jsonb.data(), e);
			return new DigitalSpecimen();
		}
	}

	private JsonNode mapToJson(JSONB jsonb) {
		return mapper.readValue(jsonb.data(), JsonNode.class);
	}

	public int[] createDigitalSpecimenRecord(Set<DigitalSpecimenRecord> digitalSpecimenRecords) {
		var queries = digitalSpecimenRecords.stream().map(this::createSpecimenQuery).toList();
		return context.batch(queries).execute();
	}

	public int[] updateDigitalSpecimenRecord(Set<DigitalSpecimenRecord> digitalSpecimenRecords) {
		var queries = digitalSpecimenRecords.stream().map(this::updateSpecimenQuery).toList();
		return context.batch(queries).execute();
	}

	private Query createSpecimenQuery(DigitalSpecimenRecord digitalSpecimenRecord) {
		return context.insertInto(DIGITAL_SPECIMEN)
			.set(DIGITAL_SPECIMEN.ID, digitalSpecimenRecord.id())
			.set(DIGITAL_SPECIMEN.TYPE, digitalSpecimenRecord.digitalSpecimenWrapper().type())
			.set(DIGITAL_SPECIMEN.VERSION, digitalSpecimenRecord.version())
			.set(DIGITAL_SPECIMEN.MIDSLEVEL, (short) digitalSpecimenRecord.midsLevel())
			.set(DIGITAL_SPECIMEN.PHYSICAL_SPECIMEN_ID,
					digitalSpecimenRecord.digitalSpecimenWrapper().physicalSpecimenID())
			.set(DIGITAL_SPECIMEN.PHYSICAL_SPECIMEN_TYPE,
					digitalSpecimenRecord.digitalSpecimenWrapper().attributes().getOdsPhysicalSpecimenIDType().value())
			.set(DIGITAL_SPECIMEN.SPECIMEN_NAME,
					digitalSpecimenRecord.digitalSpecimenWrapper().attributes().getOdsSpecimenName())
			.set(DIGITAL_SPECIMEN.ORGANIZATION_ID,
					digitalSpecimenRecord.digitalSpecimenWrapper().attributes().getOdsOrganisationID())
			.set(DIGITAL_SPECIMEN.SOURCE_SYSTEM_ID,
					digitalSpecimenRecord.digitalSpecimenWrapper().attributes().getOdsSourceSystemID())
			.set(DIGITAL_SPECIMEN.CREATED, digitalSpecimenRecord.created())
			.set(DIGITAL_SPECIMEN.LAST_CHECKED, Instant.now())
			.set(DIGITAL_SPECIMEN.MODIFIED, Instant.now())
			.set(DIGITAL_SPECIMEN.DATA, mapToJsonB(digitalSpecimenRecord))
			.set(DIGITAL_SPECIMEN.ORIGINAL_DATA,
					JSONB.valueOf(digitalSpecimenRecord.digitalSpecimenWrapper().originalAttributes().toString()));
	}

	private Query updateSpecimenQuery(DigitalSpecimenRecord digitalSpecimenRecord) {
		var query = context.update(DIGITAL_SPECIMEN)
			.set(DIGITAL_SPECIMEN.TYPE, digitalSpecimenRecord.digitalSpecimenWrapper().type())
			.set(DIGITAL_SPECIMEN.VERSION, digitalSpecimenRecord.version())
			.set(DIGITAL_SPECIMEN.MIDSLEVEL, (short) digitalSpecimenRecord.midsLevel())
			.set(DIGITAL_SPECIMEN.PHYSICAL_SPECIMEN_ID,
					digitalSpecimenRecord.digitalSpecimenWrapper().physicalSpecimenID())
			.set(DIGITAL_SPECIMEN.PHYSICAL_SPECIMEN_TYPE,
					digitalSpecimenRecord.digitalSpecimenWrapper().attributes().getOdsPhysicalSpecimenIDType().value())
			.set(DIGITAL_SPECIMEN.SPECIMEN_NAME,
					digitalSpecimenRecord.digitalSpecimenWrapper().attributes().getOdsSpecimenName())
			.set(DIGITAL_SPECIMEN.ORGANIZATION_ID,
					digitalSpecimenRecord.digitalSpecimenWrapper().attributes().getOdsOrganisationID())
			.set(DIGITAL_SPECIMEN.SOURCE_SYSTEM_ID,
					digitalSpecimenRecord.digitalSpecimenWrapper().attributes().getOdsSourceSystemID())
			.set(DIGITAL_SPECIMEN.CREATED, digitalSpecimenRecord.created())
			.set(DIGITAL_SPECIMEN.MODIFIED, Instant.now())
			.set(DIGITAL_SPECIMEN.LAST_CHECKED, Instant.now())
			.set(DIGITAL_SPECIMEN.DATA, mapToJsonB(digitalSpecimenRecord));
		if (Boolean.TRUE.equals(digitalSpecimenRecord.isDataFromSourceSystem())) {
			query = query.set(DIGITAL_SPECIMEN.ORIGINAL_DATA,
					JSONB.valueOf(digitalSpecimenRecord.digitalSpecimenWrapper().originalAttributes().toString()));
		}
		return query.where(DIGITAL_SPECIMEN.ID.eq(digitalSpecimenRecord.id()));
	}

	private JSONB mapToJsonB(DigitalSpecimenRecord digitalSpecimenRecord) {
		return JSONB.valueOf(mapper.valueToTree(digitalSpecimenRecord.digitalSpecimenWrapper().attributes())
			.toString()
			.replace("\\u0000", ""));
	}

	public void updateLastCheckedAndOriginalData(Map<String, DigitalSpecimenEvent> specimenMap) {
		var queries = specimenMap.entrySet().stream().map(entry -> {
			var query = context.update(DIGITAL_SPECIMEN).set(DIGITAL_SPECIMEN.LAST_CHECKED, Instant.now());
			if (Boolean.TRUE.equals(entry.getValue().isDataFromSourceSystem())) {
				query = query.set(DIGITAL_SPECIMEN.ORIGINAL_DATA,
						JSONB.valueOf(entry.getValue().digitalSpecimenWrapper().originalAttributes().toString()));
			}
			return query.where(DIGITAL_SPECIMEN.ID.in(entry.getKey()));
		}).toList();
		context.batch(queries).execute();
	}

	public List<DigitalSpecimenRecord> getDigitalSpecimens(List<String> specimenList) throws DisscoRepositoryException {
		try {
			return context.select(DIGITAL_SPECIMEN.asterisk())
				.from(DIGITAL_SPECIMEN)
				.where(DIGITAL_SPECIMEN.PHYSICAL_SPECIMEN_ID.in(specimenList))
				.fetch(this::mapToDigitalSpecimenRecord);
		}
		catch (DataAccessException _) {
			throw new DisscoRepositoryException("Failed to get specimen from repository: " + specimenList);
		}
	}

	public void rollbackSpecimen(String pid) {
		context.delete(DIGITAL_SPECIMEN).where(DIGITAL_SPECIMEN.ID.eq(pid)).execute();
	}

}
