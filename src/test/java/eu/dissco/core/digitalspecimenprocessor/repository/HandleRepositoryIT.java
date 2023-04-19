package eu.dissco.core.digitalspecimenprocessor.repository;

import static eu.dissco.core.digitalspecimenprocessor.database.jooq.Tables.HANDLES;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.CREATED;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.HANDLE;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.LOCAL_OBJECT_ID;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenHandleAttributes;
import static org.assertj.core.api.Assertions.assertThat;

import eu.dissco.core.digitalspecimenprocessor.domain.HandleAttribute;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.jooq.Record1;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class HandleRepositoryIT extends BaseRepositoryIT {

  private HandleRepository repository;

  @BeforeEach
  void setup() {
    repository = new HandleRepository(context);
  }

  @AfterEach
  void destroy() {
    context.truncate(HANDLES).execute();
  }

  @Test
  void testCreateHandle() {
    // Given
    var handleAttributes = givenHandleAttributes();

    // When
    repository.createHandle(HANDLE, CREATED, handleAttributes);

    // Then
    var handles = context.selectFrom(HANDLES).fetch();
    assertThat(handles).hasSize(handleAttributes.size());
  }

  @Test
  void testUpdateHandleAttributes(){
    // Given
    var handleAttributes = givenHandleAttributes();
    repository.createHandle(HANDLE, CREATED, handleAttributes);
    var updatedHandle = new HandleAttribute(11, "pidKernelMetadataLicense",
        "anotherLicenseType".getBytes(StandardCharsets.UTF_8));

    // When
    repository.updateHandleAttributes(HANDLE, CREATED, List.of(updatedHandle), true);

    // Then
    var result = context.select(HANDLES.DATA)
        .from(HANDLES)
        .where(HANDLES.HANDLE.eq(HANDLE.getBytes(StandardCharsets.UTF_8)))
        .and(HANDLES.TYPE.eq("issueNumber".getBytes(StandardCharsets.UTF_8)))
        .fetchOne(Record1::value1);
    assertThat(result).isEqualTo("2".getBytes(StandardCharsets.UTF_8));
  }

  @Test
  void testRollbackVersion(){
    // Given
    var handleAttributes = givenHandleAttributes();
    repository.createHandle(HANDLE, CREATED, handleAttributes);
    var updatedHandle = new HandleAttribute(11, "pidKernelMetadataLicense",
        "anotherLicenseType".getBytes(StandardCharsets.UTF_8));
    repository.updateHandleAttributes(HANDLE, CREATED, List.of(updatedHandle), true);
    // When

    repository.updateHandleAttributes(HANDLE, CREATED, handleAttributes, false);

    // Then
    var result = context.select(HANDLES.DATA)
        .from(HANDLES)
        .where(HANDLES.HANDLE.eq(HANDLE.getBytes(StandardCharsets.UTF_8)))
        .and(HANDLES.TYPE.eq("issueNumber".getBytes(StandardCharsets.UTF_8)))
        .fetchOne(Record1::value1);
    assertThat(result).isEqualTo("1".getBytes(StandardCharsets.UTF_8));
  }

  @Test
  void testRollbackHandle() {
    // Given
    var handleAttributes = givenHandleAttributes();
    repository.createHandle(HANDLE, CREATED, handleAttributes);

    // When
    repository.rollbackHandleCreation(HANDLE);

    // Then
    var handles = context.selectFrom(HANDLES).fetch();
    assertThat(handles).isEmpty();
  }

  @Test
  void testDSearchByPrimarySpecimenObjectIdIsPresent(){
    // Given
    var handleAttributes = givenHandleAttributes();
    repository.createHandle(HANDLE, CREATED, handleAttributes);

    // When
    var result = repository.searchByPrimarySpecimenObjectId(LOCAL_OBJECT_ID);
    var resultHandle = result.map(
        record -> new String(record.get(HANDLES.HANDLE), StandardCharsets.UTF_8)).orElse("");

    // Then
    assertThat(resultHandle).isEqualTo(HANDLE);
  }

  @Test
  void testSearchByPrimarySpecimenObjectIdNotPresent(){
    // Given
    var handleAttributes = givenHandleAttributes();
    repository.createHandle(HANDLE, CREATED, handleAttributes);

    // When
    var result = repository.searchByPrimarySpecimenObjectId("a".getBytes(StandardCharsets.UTF_8));

    // Then
    assertThat(result).isNotPresent();
  }

}
