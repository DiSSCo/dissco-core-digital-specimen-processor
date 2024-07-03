package eu.dissco.core.digitalspecimenprocessor.repository;

import static eu.dissco.core.digitalspecimenprocessor.database.jooq.Tables.SOURCE_SYSTEM;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.HANDLE;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.SOURCE_SYSTEM_ID;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.SOURCE_SYSTEM_NAME;
import static org.assertj.core.api.Assertions.assertThat;

import eu.dissco.core.digitalspecimenprocessor.database.jooq.enums.TranslatorType;
import java.time.Instant;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SourceSystemRepositoryIT extends BaseRepositoryIT {

  private SourceSystemRepository repository;

  @BeforeEach
  void setup() {
    this.repository = new SourceSystemRepository(context);
  }

  @AfterEach
  void destroy() {
    context.truncate(SOURCE_SYSTEM).execute();
  }

  @Test
  void testGetNameByID() {
    // Given
    givenInsertSourceSystem();

    // When
    var result = repository.retrieveNameByID("TEST/57Z-6PC-64W");

    // Then
    assertThat(result).isEqualTo(SOURCE_SYSTEM_NAME);
  }

  @Test
  void testGetNameByIDReturnNull() {
    // Given
    givenInsertSourceSystem();

    // When
    var result = repository.retrieveNameByID("https://hdl.handle.net/TEST/XXX-6PC-64W");

    // Then
    assertThat(result).isNull();
  }

  private void givenInsertSourceSystem() {
    context.insertInto(SOURCE_SYSTEM)
        .set(SOURCE_SYSTEM.ID, "TEST/57Z-6PC-64W")
        .set(SOURCE_SYSTEM.NAME, SOURCE_SYSTEM_NAME)
        .set(SOURCE_SYSTEM.ENDPOINT, "http://localhost:8080")
        .set(SOURCE_SYSTEM.CREATED, Instant.now())
        .set(SOURCE_SYSTEM.CREATOR, "test")
        .set(SOURCE_SYSTEM.VERSION, 1)
        .set(SOURCE_SYSTEM.MAPPING_ID, "mapping_id")
        .set(SOURCE_SYSTEM.TRANSLATOR_TYPE, TranslatorType.dwca)
        .execute();
  }

}
