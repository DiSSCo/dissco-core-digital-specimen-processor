package eu.dissco.core.digitalspecimenprocessor.repository;

import static eu.dissco.core.digitalspecimenprocessor.database.jooq.Tables.HANDLES;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfile.PRIMARY_SPECIMEN_OBJECT_ID;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.CREATED;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.HANDLE;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.LOCAL_OBJECT_ID;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenHandleAttributes;
import static org.assertj.core.api.Assertions.assertThat;

import eu.dissco.core.digitalspecimenprocessor.domain.HandleAttribute;
import eu.dissco.core.digitalspecimenprocessor.domain.IdentifierTuple;
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
    repository.createHandle(CREATED, handleAttributes);

    // Then
    var handles = context.selectFrom(HANDLES).fetch();
    assertThat(handles).hasSize(handleAttributes.size());
  }

  @Test
  void testUpdateHandleAttributes(){
    // Given
    var handleAttributes = givenHandleAttributes();
    repository.createHandle(CREATED, handleAttributes);
    var updatedHandle = new HandleAttribute(11, "pidKernelMetadataLicense",
        "anotherLicenseType".getBytes(StandardCharsets.UTF_8), HANDLE);

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
    repository.createHandle(CREATED, handleAttributes);
    var updatedHandle = new HandleAttribute(11, "pidKernelMetadataLicense",
        "anotherLicenseType".getBytes(StandardCharsets.UTF_8), HANDLE);
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
    repository.createHandle(CREATED, handleAttributes);

    // When
    repository.rollbackHandleCreation(HANDLE);

    // Then
    var handles = context.selectFrom(HANDLES).fetch();
    assertThat(handles).isEmpty();
  }

  @Test
  void testSearchByPrimarySpecimenObjectIdBatchIsPresent(){
    // Given
    var attribute = List.of(new HandleAttribute(PRIMARY_SPECIMEN_OBJECT_ID.getIndex(), PRIMARY_SPECIMEN_OBJECT_ID.getAttribute(), LOCAL_OBJECT_ID, HANDLE));
    repository.createHandle(CREATED, attribute);
    var expected = List.of(new IdentifierTuple(HANDLE, new String(LOCAL_OBJECT_ID, StandardCharsets.UTF_8)));

    // When
    var result = repository.searchByPrimarySpecimenObjectId(List.of(LOCAL_OBJECT_ID));

    // Then
    assertThat(result).isEqualTo(expected);
  }

  @Test
  void testSearchByPrimarySpecimenObjectIdBatchNotPresent(){
    // Given
    var attribute = List.of(new HandleAttribute(PRIMARY_SPECIMEN_OBJECT_ID.getIndex(), PRIMARY_SPECIMEN_OBJECT_ID.getAttribute(), LOCAL_OBJECT_ID, HANDLE));
    repository.createHandle(CREATED, attribute);

    // When
    var result = repository.searchByPrimarySpecimenObjectId(List.of("Not an Identifier".getBytes(
        StandardCharsets.UTF_8)));

    // Then
    assertThat(result).isEmpty();
  }

  @Test
  void TestUpdateHandleAttributesBatch(){
    // Given
    repository.createHandle(CREATED, givenHandleAttributes());
    var newAttributes = List.of(
        new HandleAttribute(PRIMARY_SPECIMEN_OBJECT_ID.getIndex(),PRIMARY_SPECIMEN_OBJECT_ID.getAttribute(),"New value".getBytes(
            StandardCharsets.UTF_8), HANDLE),
        new HandleAttribute(400, "test", "test".getBytes(StandardCharsets.UTF_8), HANDLE)
    );

    // When
    repository.updateHandleAttributesBatch(CREATED, List.of(newAttributes));
    var result = context.select(HANDLES.asterisk()).from(HANDLES).fetch();

    // Then
    assertThat(result).hasSize(6);
  }


}
