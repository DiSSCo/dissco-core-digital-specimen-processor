package eu.dissco.core.digitalspecimenprocessor.component;

import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.SOURCE_SYSTEM_ID;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.SOURCE_SYSTEM_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;

import eu.dissco.core.digitalspecimenprocessor.repository.SourceSystemRepository;
import eu.dissco.core.digitalspecimenprocessor.utils.TestUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SourceSystemNameComponentTest {

  @Mock
  private SourceSystemRepository repository;

  private SourceSystemNameComponent component;

  @BeforeEach
  void setup() {
    this.component = new SourceSystemNameComponent(repository);
  }

  @Test
  void testGetSourceSystemName() {
    // Given
    given(repository.retrieveNameByID("TEST/57Z-6PC-64W")).willReturn(SOURCE_SYSTEM_NAME);

    // When
    var name = component.getSourceSystemName(SOURCE_SYSTEM_ID);

    // Then
    assertThat(name).isEqualTo(SOURCE_SYSTEM_NAME);
  }


}
