package eu.dissco.core.digitalspecimenprocessor.repository;

import static eu.dissco.core.digitalspecimenprocessor.database.jooq.Tables.NEW_DIGITAL_SPECIMEN;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dissco.core.digitalspecimenprocessor.domain.DigitalSpecimen;
import eu.dissco.core.digitalspecimenprocessor.domain.DigitalSpecimenRecord;
import eu.dissco.core.digitalspecimenprocessor.exception.DisscoJsonBMappingException;
import eu.dissco.core.digitalspecimenprocessor.exception.DisscoRepositoryException;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.jooq.DSLContext;
import org.jooq.JSONB;
import org.jooq.Query;
import org.jooq.Record;
import org.jooq.exception.DataAccessException;
import org.springframework.stereotype.Repository;

@Repository
@RequiredArgsConstructor
public class DigitalSpecimenRepository {

  private final DSLContext context;
  private final ObjectMapper mapper;

  private DigitalSpecimenRecord mapDigitalSpecimen(Record dbRecord) {
    DigitalSpecimen digitalSpecimen = null;
    digitalSpecimen = new DigitalSpecimen(dbRecord.get(NEW_DIGITAL_SPECIMEN.TYPE),
        dbRecord.get(NEW_DIGITAL_SPECIMEN.PHYSICAL_SPECIMEN_ID),
        dbRecord.get(NEW_DIGITAL_SPECIMEN.PHYSICAL_SPECIMEN_TYPE),
        dbRecord.get(NEW_DIGITAL_SPECIMEN.SPECIMEN_NAME),
        dbRecord.get(NEW_DIGITAL_SPECIMEN.ORGANIZATION_ID),
        dbRecord.get(NEW_DIGITAL_SPECIMEN.DATASET),
        dbRecord.get(NEW_DIGITAL_SPECIMEN.PHYSICAL_SPECIMEN_COLLECTION),
        dbRecord.get(NEW_DIGITAL_SPECIMEN.SOURCE_SYSTEM_ID),
        mapToJson(dbRecord.get(NEW_DIGITAL_SPECIMEN.DATA)),
        mapToJson(dbRecord.get(NEW_DIGITAL_SPECIMEN.ORIGINAL_DATA)),
        dbRecord.get(NEW_DIGITAL_SPECIMEN.DWCA_ID));

    return new DigitalSpecimenRecord(
        dbRecord.get(NEW_DIGITAL_SPECIMEN.ID),
        dbRecord.get(NEW_DIGITAL_SPECIMEN.MIDSLEVEL),
        dbRecord.get(NEW_DIGITAL_SPECIMEN.VERSION),
        dbRecord.get(NEW_DIGITAL_SPECIMEN.CREATED),
        digitalSpecimen);
  }

  private JsonNode mapToJson(JSONB jsonb) {
    try {
      return mapper.readTree(jsonb.data());
    } catch (JsonProcessingException e) {
      throw new DisscoJsonBMappingException(
          "Failed to parse jsonb field to json: " + jsonb.data(), e);
    }
  }


  public int[] createDigitalSpecimenRecord(
      Collection<DigitalSpecimenRecord> digitalSpecimenRecords) {
    var queries = digitalSpecimenRecords.stream().map(this::specimenToQuery).toList();
    return context.batch(queries).execute();
  }

  private Query specimenToQuery(DigitalSpecimenRecord digitalSpecimenRecord) {
    return context.insertInto(NEW_DIGITAL_SPECIMEN)
        .set(NEW_DIGITAL_SPECIMEN.ID, digitalSpecimenRecord.id())
        .set(NEW_DIGITAL_SPECIMEN.TYPE, digitalSpecimenRecord.digitalSpecimen().type())
        .set(NEW_DIGITAL_SPECIMEN.VERSION, digitalSpecimenRecord.version())
        .set(NEW_DIGITAL_SPECIMEN.MIDSLEVEL, (short) digitalSpecimenRecord.midsLevel())
        .set(NEW_DIGITAL_SPECIMEN.PHYSICAL_SPECIMEN_ID,
            digitalSpecimenRecord.digitalSpecimen().physicalSpecimenId())
        .set(NEW_DIGITAL_SPECIMEN.PHYSICAL_SPECIMEN_TYPE,
            digitalSpecimenRecord.digitalSpecimen().physicalSpecimenIdType())
        .set(NEW_DIGITAL_SPECIMEN.SPECIMEN_NAME,
            digitalSpecimenRecord.digitalSpecimen().specimenName())
        .set(NEW_DIGITAL_SPECIMEN.ORGANIZATION_ID,
            digitalSpecimenRecord.digitalSpecimen().organizationId())
        .set(NEW_DIGITAL_SPECIMEN.PHYSICAL_SPECIMEN_COLLECTION,
            digitalSpecimenRecord.digitalSpecimen().physicalSpecimenCollection())
        .set(NEW_DIGITAL_SPECIMEN.DATASET, digitalSpecimenRecord.digitalSpecimen().datasetId())
        .set(NEW_DIGITAL_SPECIMEN.SOURCE_SYSTEM_ID,
            digitalSpecimenRecord.digitalSpecimen().sourceSystemId())
        .set(NEW_DIGITAL_SPECIMEN.CREATED, digitalSpecimenRecord.created())
        .set(NEW_DIGITAL_SPECIMEN.LAST_CHECKED, Instant.now())
        .set(NEW_DIGITAL_SPECIMEN.DATA,
            JSONB.valueOf(digitalSpecimenRecord.digitalSpecimen().data().toString()))
        .set(NEW_DIGITAL_SPECIMEN.ORIGINAL_DATA,
            JSONB.valueOf(digitalSpecimenRecord.digitalSpecimen().originalData().toString()))
        .set(NEW_DIGITAL_SPECIMEN.DWCA_ID, digitalSpecimenRecord.digitalSpecimen().dwcaId());
  }

  public int updateLastChecked(List<String> currentDigitalSpecimen) {
    return context.update(NEW_DIGITAL_SPECIMEN)
        .set(NEW_DIGITAL_SPECIMEN.LAST_CHECKED, Instant.now())
        .where(NEW_DIGITAL_SPECIMEN.ID.in(currentDigitalSpecimen))
        .execute();
  }

  public List<DigitalSpecimenRecord> getDigitalSpecimens(List<String> specimenList)
      throws DisscoRepositoryException {
    try {
      return context.select(NEW_DIGITAL_SPECIMEN.asterisk())
          .distinctOn(NEW_DIGITAL_SPECIMEN.ID)
          .from(NEW_DIGITAL_SPECIMEN)
          .where(NEW_DIGITAL_SPECIMEN.PHYSICAL_SPECIMEN_ID.in(specimenList))
          .orderBy(NEW_DIGITAL_SPECIMEN.ID, NEW_DIGITAL_SPECIMEN.VERSION.desc())
          .fetch(this::mapDigitalSpecimen);
    } catch (DataAccessException ex) {
      throw new DisscoRepositoryException(
          "Failed to get specimen from repository: " + specimenList);
    }
  }

  public void rollbackSpecimen(DigitalSpecimenRecord digitalSpecimenRecord) {
    context.delete(NEW_DIGITAL_SPECIMEN)
        .where(NEW_DIGITAL_SPECIMEN.ID.eq(digitalSpecimenRecord.id()))
        .execute();
  }

  public void deleteVersion(DigitalSpecimenRecord digitalSpecimenRecord) {
    context.delete(NEW_DIGITAL_SPECIMEN)
        .where(NEW_DIGITAL_SPECIMEN.ID.eq(digitalSpecimenRecord.id()))
        .and(NEW_DIGITAL_SPECIMEN.VERSION.eq(digitalSpecimenRecord.version()))
        .execute();
  }
}
