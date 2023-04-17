package eu.dissco.core.digitalspecimenprocessor.repository;

import static eu.dissco.core.digitalspecimenprocessor.database.jooq.Tables.HANDLES;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoUtils.FIELD_IDX;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoUtils.PRIMARY_SPECIMEN_OBJECT_ID;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.CREATED;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.HANDLE;
import static org.assertj.core.api.Assertions.as;
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

  private static final byte[] OBJECT_ID = "0x12:RMNH.QW99".getBytes(StandardCharsets.UTF_8);

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
    var result = repository.searchByPrimarySpecimenObjectId(OBJECT_ID);
    var resultHandle = result.map(
        record -> new String(record.get(HANDLES.HANDLE), StandardCharsets.UTF_8)).orElse("");

    // Then
    assertThat(resultHandle).isEqualTo(HANDLE);
  }

  @Test
  void testDSearchByPrimarySpecimenObjectIdNotPresent(){
    // Given
    var handleAttributes = givenHandleAttributes();
    repository.createHandle(HANDLE, CREATED, handleAttributes);

    // When
    var result = repository.searchByPrimarySpecimenObjectId("a".getBytes(StandardCharsets.UTF_8));

    // Then
    assertThat(result).isNotPresent();
  }

  private List<HandleAttribute> givenHandleAttributes() {
    return List.of(
        new HandleAttribute(1, "pid",
            ("https://hdl.handle.net/" + HANDLE).getBytes(StandardCharsets.UTF_8)),
        new HandleAttribute(11, "pidKernelMetadataLicense",
            "https://creativecommons.org/publicdomain/zero/1.0/".getBytes(StandardCharsets.UTF_8)),
        new HandleAttribute(FIELD_IDX.get(PRIMARY_SPECIMEN_OBJECT_ID),
            PRIMARY_SPECIMEN_OBJECT_ID,
            OBJECT_ID),
        new HandleAttribute(7, "issueNumber", "1".getBytes(StandardCharsets.UTF_8)),
        new HandleAttribute(100, "HS_ADMIN", "TEST_ADMIN_STRING".getBytes(StandardCharsets.UTF_8))
    );
  }

}
