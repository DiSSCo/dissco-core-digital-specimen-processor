package eu.dissco.core.digitalspecimenprocessor.repository;

import static eu.dissco.core.digitalspecimenprocessor.database.jooq.Tables.NEW_DIGITAL_SPECIMEN;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dissco.core.digitalspecimenprocessor.domain.DigitalSpecimen;
import eu.dissco.core.digitalspecimenprocessor.domain.DigitalSpecimenRecord;
import java.time.Instant;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import org.jooq.DSLContext;
import org.jooq.JSONB;
import org.jooq.Record;
import org.springframework.stereotype.Repository;

@Repository
@RequiredArgsConstructor
public class DigitalSpecimenRepository {

  private final DSLContext context;
  private final ObjectMapper mapper;

  public Optional<DigitalSpecimenRecord> getDigitalSpecimen(String physicalSpecimenId) {
    return context.select(NEW_DIGITAL_SPECIMEN.asterisk())
        .distinctOn(NEW_DIGITAL_SPECIMEN.ID)
        .from(NEW_DIGITAL_SPECIMEN)
        .where(NEW_DIGITAL_SPECIMEN.PHYSICAL_SPECIMEN_ID.eq(physicalSpecimenId))
        .orderBy(NEW_DIGITAL_SPECIMEN.ID, NEW_DIGITAL_SPECIMEN.VERSION.desc())
        .fetchOptional(this::mapDigitalSpecimen);
  }

  private DigitalSpecimenRecord mapDigitalSpecimen(Record dbRecord) {
    DigitalSpecimen digitalSpecimen = null;
    try {
      digitalSpecimen = new DigitalSpecimen(dbRecord.get(NEW_DIGITAL_SPECIMEN.TYPE),
          dbRecord.get(NEW_DIGITAL_SPECIMEN.PHYSICAL_SPECIMEN_ID),
          dbRecord.get(NEW_DIGITAL_SPECIMEN.PHYSICAL_SPECIMEN_TYPE),
          dbRecord.get(NEW_DIGITAL_SPECIMEN.SPECIMEN_NAME),
          dbRecord.get(NEW_DIGITAL_SPECIMEN.ORGANIZATION_ID),
          dbRecord.get(NEW_DIGITAL_SPECIMEN.DATASET),
          dbRecord.get(NEW_DIGITAL_SPECIMEN.PHYSICAL_SPECIMEN_COLLECTION),
          dbRecord.get(NEW_DIGITAL_SPECIMEN.SOURCE_SYSTEM_ID),
          mapper.readTree(dbRecord.get(NEW_DIGITAL_SPECIMEN.DATA).data()),
          mapper.readTree(dbRecord.get(NEW_DIGITAL_SPECIMEN.ORIGINAL_DATA).data()),
          dbRecord.get(NEW_DIGITAL_SPECIMEN.DWCA_ID));
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
    return new DigitalSpecimenRecord(dbRecord.get(NEW_DIGITAL_SPECIMEN.ID),
        dbRecord.get(NEW_DIGITAL_SPECIMEN.MIDSLEVEL), dbRecord.get(NEW_DIGITAL_SPECIMEN.VERSION),
        digitalSpecimen);
  }

  public int createDigitalSpecimenRecord(DigitalSpecimenRecord digitalSpecimenRecord) {
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
        .set(NEW_DIGITAL_SPECIMEN.CREATED, Instant.now())
        .set(NEW_DIGITAL_SPECIMEN.LAST_CHECKED, Instant.now()).set(NEW_DIGITAL_SPECIMEN.DATA,
            JSONB.valueOf(digitalSpecimenRecord.digitalSpecimen().data().toString()))
        .set(NEW_DIGITAL_SPECIMEN.ORIGINAL_DATA,
            JSONB.valueOf(digitalSpecimenRecord.digitalSpecimen().originalData().toString()))
        .set(NEW_DIGITAL_SPECIMEN.DWCA_ID, digitalSpecimenRecord.digitalSpecimen().dwcaId())
        .execute();
  }

  public int updateLastChecked(DigitalSpecimenRecord currentDigitalSpecimen) {
    return context.update(NEW_DIGITAL_SPECIMEN)
        .set(NEW_DIGITAL_SPECIMEN.LAST_CHECKED, Instant.now())
        .where(NEW_DIGITAL_SPECIMEN.ID.eq(currentDigitalSpecimen.id())).execute();
  }
}
