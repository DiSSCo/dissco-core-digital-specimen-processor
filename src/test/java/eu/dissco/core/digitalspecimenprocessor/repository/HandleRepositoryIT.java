package eu.dissco.core.digitalspecimenprocessor.repository;

import static eu.dissco.core.digitalspecimenprocessor.database.jooq.Tables.HANDLES;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.CREATED;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.HANDLE;
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

  private List<HandleAttribute> givenHandleAttributes() {
    return List.of(
        new HandleAttribute(1, "pid",
            ("https://hdl.handle.net/" + HANDLE).getBytes(StandardCharsets.UTF_8)),
        new HandleAttribute(11, "pidKernelMetadataLicense",
            "https://creativecommons.org/publicdomain/zero/1.0/".getBytes(StandardCharsets.UTF_8)),
        new HandleAttribute(7, "issueNumber", "1".getBytes(StandardCharsets.UTF_8)),
        new HandleAttribute(100, "HS_ADMIN", "TEST_ADMIN_STRING".getBytes(StandardCharsets.UTF_8))
    );
  }

}
