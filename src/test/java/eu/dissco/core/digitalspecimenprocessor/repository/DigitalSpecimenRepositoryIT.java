package eu.dissco.core.digitalspecimenprocessor.repository;

import static eu.dissco.core.digitalspecimenprocessor.database.jooq.Tables.NEW_DIGITAL_SPECIMEN;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.HANDLE;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MAPPER;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.PHYSICAL_SPECIMEN_ID;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.SECOND_HANDLE;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.THIRD_HANDLE;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenRecord;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mockStatic;

import eu.dissco.core.digitalspecimenprocessor.exception.DisscoRepositoryException;
import java.sql.BatchUpdateException;
import java.time.Instant;
import java.util.List;
import org.jooq.Record1;
import org.jooq.exception.DataAccessException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.postgresql.util.PSQLException;

class DigitalSpecimenRepositoryIT extends BaseRepositoryIT {

  private static final Instant UPDATED_TIMESTAMP = Instant.parse("2022-11-02T13:05:24.00Z");


  private DigitalSpecimenRepository repository;

  @BeforeEach
  void setup() {
    repository = new DigitalSpecimenRepository(context, MAPPER);
  }

  @AfterEach
  void destroy() {
    context.truncate(NEW_DIGITAL_SPECIMEN).execute();
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
        List.of(
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
        List.of(
            givenDigitalSpecimenRecord(),
            givenDigitalSpecimenRecord("20.5000.1025/XXX-XXX-XXX", "TEST_1"),
            givenDigitalSpecimenRecord("20.5000.1025/YYY-YYY-YYY", "TEST_2")));

    // When
    var result = repository.createDigitalSpecimenRecord(List.of(givenDigitalSpecimenRecord(2)));

    // Then
    assertThat(result).isEqualTo(new int[]{1});
  }

  @Test
  void testUpdateLastChecked() {
    // Given
    repository.createDigitalSpecimenRecord(
        List.of(
            givenDigitalSpecimenRecord(),
            givenDigitalSpecimenRecord(SECOND_HANDLE, "TEST_1"),
            givenDigitalSpecimenRecord(THIRD_HANDLE, "TEST_2")));

    // When
    try (MockedStatic<Instant> mockedStatic = mockStatic(Instant.class)) {
      mockedStatic.when(Instant::now).thenReturn(UPDATED_TIMESTAMP);
      repository.updateLastChecked(List.of(HANDLE));
    }

    // Then
    var result = context.select(NEW_DIGITAL_SPECIMEN.LAST_CHECKED)
        .from(NEW_DIGITAL_SPECIMEN)
        .where(NEW_DIGITAL_SPECIMEN.ID.eq(HANDLE)).fetchOne(Record1::value1);
    assertThat(result).isEqualTo(UPDATED_TIMESTAMP);
  }

  @Test
  void testInsertDuplicateSpecimens() {
    // Given
    var records = List.of(
        givenDigitalSpecimenRecord(),
        givenDigitalSpecimenRecord(SECOND_HANDLE, "TEST_1"),
        givenDigitalSpecimenRecord(SECOND_HANDLE, "TEST_2"));

    // When
    var exception = assertThrows(DataAccessException.class, () -> {
      repository.createDigitalSpecimenRecord(records);
    });

    // Then
    assertThat(exception).hasCauseInstanceOf(BatchUpdateException.class).hasRootCauseInstanceOf(
        PSQLException.class);
  }

  @Test
  void testRollbackSpecimen() throws DisscoRepositoryException {
    // Given
    repository.createDigitalSpecimenRecord(
        List.of(
            givenDigitalSpecimenRecord(),
            givenDigitalSpecimenRecord(SECOND_HANDLE, "TEST_1"),
            givenDigitalSpecimenRecord(THIRD_HANDLE, "TEST_2")));

    // When
    repository.rollbackSpecimen(HANDLE);

    // Then
    var result = repository.getDigitalSpecimens(List.of(PHYSICAL_SPECIMEN_ID));
    assertThat(result).isEmpty();
  }

  @Test
  void testRollbackVersion() throws DisscoRepositoryException {
    // Given
    repository.createDigitalSpecimenRecord(
        List.of(
            givenDigitalSpecimenRecord(),
            givenDigitalSpecimenRecord("20.5000.1025/XXX-XXX-XXX", "TEST_1"),
            givenDigitalSpecimenRecord("20.5000.1025/YYY-YYY-YYY", "TEST_2")));
    repository.createDigitalSpecimenRecord(List.of(givenDigitalSpecimenRecord(2)));

    // When
    repository.deleteVersion(givenDigitalSpecimenRecord(2));

    // Then
    var result = repository.getDigitalSpecimens(List.of(PHYSICAL_SPECIMEN_ID));
    assertThat(result.get(0)).isEqualTo(givenDigitalSpecimenRecord());
  }

}
