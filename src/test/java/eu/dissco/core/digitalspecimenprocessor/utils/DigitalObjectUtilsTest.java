package eu.dissco.core.digitalspecimenprocessor.utils;

import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.DOI_PREFIX;
import static org.assertj.core.api.Assertions.assertThat;

import eu.dissco.core.digitalspecimenprocessor.util.DigitalObjectUtils;
import org.junit.jupiter.api.Test;

class DigitalObjectUtilsTest {

  @Test
  void testFlattenToDigitalSpecimen() {
    // Given
    var digitalSpecimenRecord = TestUtils.givenDigitalSpecimenRecord();

    // When
    var digitalSpecimen = DigitalObjectUtils.flattenToDigitalSpecimen(digitalSpecimenRecord);

    // Then
    assertThat(digitalSpecimen.getId()).isEqualTo(DOI_PREFIX + digitalSpecimenRecord.id());
    assertThat(digitalSpecimen.getDctermsIdentifier()).isEqualTo(DOI_PREFIX + digitalSpecimenRecord.id());
    assertThat(digitalSpecimen.getOdsVersion()).isEqualTo(digitalSpecimenRecord.version());
    assertThat(digitalSpecimen.getOdsMidsLevel()).isEqualTo(digitalSpecimenRecord.midsLevel());
    assertThat(digitalSpecimen.getDctermsCreated()).isEqualTo(digitalSpecimenRecord.created());
  }

}
