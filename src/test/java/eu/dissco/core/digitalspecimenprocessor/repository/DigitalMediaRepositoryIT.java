package eu.dissco.core.digitalspecimenprocessor.repository;

import static eu.dissco.core.digitalspecimenprocessor.database.jooq.Tables.DIGITAL_MEDIA_OBJECT;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.CREATED;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.HANDLE;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MAPPER;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MEDIA_PID;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MEDIA_URL;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.jooq.JSONB;
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
  /*

  @Test
  void testGetDigitalMediaDois() {
    // Given
    populateDb();
    var expected = givenMediaPidResponse();

    // When
    var result = mediaRepository.getDigitalMediaUrisFromIdKey(List.of(MEDIA_PID));

    // Then
    assertThat(result).isEqualTo(expected);
  }

  @Test
  void testRemoveSpecimenRelationshipFromMedia() {
    // Given
    populateDb();

    // When
    mediaRepository.removeSpecimenRelationshipsFromMedia(List.of(MEDIA_PID));
    var result = context.select(DIGITAL_MEDIA_OBJECT.DIGITAL_SPECIMEN_ID)
        .from(DIGITAL_MEDIA_OBJECT)
        .where(DIGITAL_MEDIA_OBJECT.ID.eq(MEDIA_PID))
        .fetchOne(DIGITAL_MEDIA_OBJECT.DIGITAL_SPECIMEN_ID);

    // Then
    assertThat(result).isEqualTo("UNKNOWN");
  }

  private void populateDb() {
    context.insertInto(DIGITAL_MEDIA_OBJECT)
        .set(DIGITAL_MEDIA_OBJECT.ID, MEDIA_PID)
        .set(DIGITAL_MEDIA_OBJECT.VERSION, 1)
        .set(DIGITAL_MEDIA_OBJECT.TYPE, "image")
        .set(DIGITAL_MEDIA_OBJECT.DIGITAL_SPECIMEN_ID, HANDLE)
        .set(DIGITAL_MEDIA_OBJECT.MEDIA_URL, MEDIA_URL)
        .set(DIGITAL_MEDIA_OBJECT.CREATED, CREATED)
        .set(DIGITAL_MEDIA_OBJECT.LAST_CHECKED, CREATED)
        .set(DIGITAL_MEDIA_OBJECT.DATA, JSONB.jsonb("{}"))
        .set(DIGITAL_MEDIA_OBJECT.ORIGINAL_DATA, JSONB.jsonb("{}"))
        .execute();
  } */


}
