package eu.dissco.core.digitalspecimenprocessor.repository;

import static eu.dissco.core.digitalspecimenprocessor.database.jooq.Tables.DIGITAL_MEDIA_OBJECT;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MAPPER;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MEDIA_PID;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MEDIA_URL;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalMediaRecord;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalMediaRecordNoMas;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Set;
import org.jooq.Record1;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DigitalMediaRepositoryIT extends BaseRepositoryIT {

  private DigitalMediaRepository mediaRepository;


  @BeforeEach
  void setup() {
    mediaRepository = new DigitalMediaRepository(context, MAPPER);
  }

  @AfterEach
  void destroy() {
    context.truncate(DIGITAL_MEDIA_OBJECT).execute();
  }

  @Test
  void testCreateDigitalMediaRecord() {
    // Given
    var digitalMedia = givenDigitalMediaRecord();

    // When
    mediaRepository.createDigitalMediaRecord(Set.of(digitalMedia));
    var dbRecord = context.select(DIGITAL_MEDIA_OBJECT.asterisk())
        .from(DIGITAL_MEDIA_OBJECT)
        .where(DIGITAL_MEDIA_OBJECT.ID.eq(MEDIA_PID))
        .fetchOne();
    var result = dbRecord.get(DIGITAL_MEDIA_OBJECT.MEDIA_URL);

    // Then
    assertThat(result).isEqualTo(MEDIA_URL);
  }

  @Test
  void testGetExistingDigitalMedia() {
    // Given
    mediaRepository.createDigitalMediaRecord(Set.of(givenDigitalMediaRecord()));

    // When
    var result = mediaRepository.getExistingDigitalMedia(Set.of(MEDIA_URL));

    // Then
    assertThat(result).isEqualTo(List.of(givenDigitalMediaRecordNoMas()));
  }

  @Test
  void testRollbackMedia() {
    // Given
    mediaRepository.createDigitalMediaRecord(Set.of(givenDigitalMediaRecord()));

    // When
    mediaRepository.rollBackDigitalMedia(MEDIA_PID);
    var result = context.select(DIGITAL_MEDIA_OBJECT.asterisk())
        .from(DIGITAL_MEDIA_OBJECT)
        .where(DIGITAL_MEDIA_OBJECT.ID.eq(MEDIA_PID))
        .fetchOne();

    // Then
    assertThat(result).isNull();
  }

  @Test
  void testUpdateLastChecked() {
    // Given
    mediaRepository.createDigitalMediaRecord(Set.of(givenDigitalMediaRecord()));
    var lastChecked = context.select(DIGITAL_MEDIA_OBJECT.LAST_CHECKED)
        .from(DIGITAL_MEDIA_OBJECT)
        .where(DIGITAL_MEDIA_OBJECT.ID.eq(MEDIA_PID))
        .fetchOne(Record1::value1);

    // When
    mediaRepository.updateLastChecked(List.of(MEDIA_PID));
    var result = context.select(DIGITAL_MEDIA_OBJECT.LAST_CHECKED)
        .from(DIGITAL_MEDIA_OBJECT)
        .where(DIGITAL_MEDIA_OBJECT.ID.eq(MEDIA_PID))
        .fetchOne(Record1::value1);

    // Then
    assertThat(result).isAfter(lastChecked);
  }

  @Test
  void testGetExistingDigitalMediaByDoi() {
    // Given
    mediaRepository.createDigitalMediaRecord(Set.of(givenDigitalMediaRecord()));

    // When
    var result = mediaRepository.getExistingDigitalMediaByDoi(Set.of(MEDIA_PID));

    // Then
    assertThat(result).isEqualTo(List.of(givenDigitalMediaRecordNoMas()));
  }

}
