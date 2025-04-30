package eu.dissco.core.digitalspecimenprocessor.repository;

import static eu.dissco.core.digitalspecimenprocessor.database.jooq.Tables.DIGITAL_SPECIMEN;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.HANDLE;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MAPPER;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.PHYSICAL_SPECIMEN_ID;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.SECOND_HANDLE;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.THIRD_HANDLE;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenRecord;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenRecordNoOriginalData;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import eu.dissco.core.digitalspecimenprocessor.exception.DisscoRepositoryException;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import org.jooq.Record1;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DigitalSpecimenRepositoryIT extends BaseRepositoryIT {

  private static final Instant UPDATED_TIMESTAMP = Instant.parse("2022-11-02T13:05:24.00Z");

  private DigitalSpecimenRepository repository;

  @BeforeEach
  void setup() {
    repository = new DigitalSpecimenRepository(context, MAPPER);
  }

  @AfterEach
  void destroy() {
    context.truncate(DIGITAL_SPECIMEN).execute();
  }

  @Test
  void testGetDigitalSpecimensEmpty() throws DisscoRepositoryException {
    // Given

    // When
    var result = repository.getDigitalSpecimens(List.of(PHYSICAL_SPECIMEN_ID));

    // Then
    assertThat(result).isEmpty();
  }

  @Test
  void testGetDigitalSpecimens() throws DisscoRepositoryException {
    // Given
    repository.createDigitalSpecimenRecord(
        Set.of(
            givenDigitalSpecimenRecord(),
            givenDigitalSpecimenRecord("20.5000.1025/XXX-XXX-XXX", "TEST_1"),
            givenDigitalSpecimenRecord("20.5000.1025/YYY-YYY-YYY", "TEST_2")));

    // When
    var result = repository.getDigitalSpecimens(List.of(PHYSICAL_SPECIMEN_ID));

    // Then
    assertThat(result.get(0)).isEqualTo(givenDigitalSpecimenRecord());
  }

  @Test
  void testUpdateVersionSpecimens() {
    // Given
    repository.createDigitalSpecimenRecord(
        Set.of(
            givenDigitalSpecimenRecord(),
            givenDigitalSpecimenRecord("20.5000.1025/XXX-XXX-XXX", "TEST_1"),
            givenDigitalSpecimenRecord("20.5000.1025/YYY-YYY-YYY", "TEST_2")));

    // When
    var result = repository.createDigitalSpecimenRecord(
        Set.of(givenDigitalSpecimenRecord(2, false)));

    // Then
    assertThat(result).isEqualTo(new int[]{1});
  }

  @Test
  void testUpdateLastChecked() {
    // Given
    repository.createDigitalSpecimenRecord(
        Set.of(
            givenDigitalSpecimenRecord(),
            givenDigitalSpecimenRecord(SECOND_HANDLE, "TEST_1"),
            givenDigitalSpecimenRecord(THIRD_HANDLE, "TEST_2")));

    // When
    repository.updateLastChecked(List.of(HANDLE));
    var result = context.select(DIGITAL_SPECIMEN.LAST_CHECKED)
        .from(DIGITAL_SPECIMEN)
        .where(DIGITAL_SPECIMEN.ID.eq(HANDLE)).fetchOne(Record1::value1);

    // Then
    assertThat(result).isAfter(UPDATED_TIMESTAMP);
  }


  @Test
  void testUpsertSpecimens() {
    // Given
    var records = Set.of(
        givenDigitalSpecimenRecord(),
        givenDigitalSpecimenRecord(SECOND_HANDLE, "TEST_1"));
    var upsertRecord = Set.of(givenDigitalSpecimenRecord(SECOND_HANDLE, "TEST_2"));

    // When
    repository.createDigitalSpecimenRecord(records);
    repository.createDigitalSpecimenRecord(upsertRecord);

    // Then
    var result = context.select(DIGITAL_SPECIMEN.PHYSICAL_SPECIMEN_ID)
        .from(DIGITAL_SPECIMEN).where(DIGITAL_SPECIMEN.ID.eq(SECOND_HANDLE))
        .fetchOne(Record1::value1);
    assertThat(result).isEqualTo("TEST_2");
  }

  @Test
  void testUpsertSpecimensOriginalDataOnly() throws JsonProcessingException{
    // Given
    var firstRecord = givenDigitalSpecimenRecordNoOriginalData();
    var records = Set.of(
        firstRecord,
        givenDigitalSpecimenRecord());

    // When
    repository.createDigitalSpecimenRecord(records);

    // Then
    var result = MAPPER.readTree(context.select(DIGITAL_SPECIMEN.ORIGINAL_DATA)
        .from(DIGITAL_SPECIMEN).where(DIGITAL_SPECIMEN.ID.eq(HANDLE))
        .fetchOne(Record1::value1).data());

    assertThat(result).isEqualTo(MAPPER.createObjectNode());
  }


  @Test
  void testCreateWithInvalidUnicode() {
    // Given
    var ds = givenDigitalSpecimenRecord();
    ds.digitalSpecimenWrapper().attributes().setDwcCollectionCode("\u0000");

    // When
    repository.createDigitalSpecimenRecord(Set.of(ds));

    // Then
    var result = context.select(DIGITAL_SPECIMEN.PHYSICAL_SPECIMEN_ID)
        .from(DIGITAL_SPECIMEN).where(DIGITAL_SPECIMEN.ID.eq(HANDLE))
        .fetchOne(Record1::value1);
    assertThat(result).isEqualTo(PHYSICAL_SPECIMEN_ID);
  }

  @Test
  void testRollbackSpecimen() throws DisscoRepositoryException {
    // Given
    repository.createDigitalSpecimenRecord(
        Set.of(
            givenDigitalSpecimenRecord(),
            givenDigitalSpecimenRecord(SECOND_HANDLE, "TEST_1"),
            givenDigitalSpecimenRecord(THIRD_HANDLE, "TEST_2")));

    // When
    repository.rollbackSpecimen(HANDLE);

    // Then
    var result = repository.getDigitalSpecimens(List.of(PHYSICAL_SPECIMEN_ID));
    assertThat(result).isEmpty();
  }

}
