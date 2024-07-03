package eu.dissco.core.digitalspecimenprocessor.utils;

import static org.assertj.core.api.Assertions.assertThat;

import eu.dissco.core.digitalspecimenprocessor.util.DigitalSpecimenUtils;
import org.junit.jupiter.api.Test;

class DigitalSpecimenUtilsTest {

  @Test
  void testFlattenToDigitalSpecimen() {
    // Given
    var digitalSpecimenRecord = TestUtils.givenDigitalSpecimenRecord();

    // When
    var digitalSpecimen = DigitalSpecimenUtils.flattenToDigitalSpecimen(digitalSpecimenRecord);

    // Then
    assertThat(digitalSpecimen.getId()).isEqualTo(digitalSpecimenRecord.id());
    assertThat(digitalSpecimen.getOdsID()).isEqualTo(digitalSpecimenRecord.id());
    assertThat(digitalSpecimen.getOdsVersion()).isEqualTo(digitalSpecimenRecord.version());
    assertThat(digitalSpecimen.getOdsMidsLevel()).isEqualTo(digitalSpecimenRecord.midsLevel());
    assertThat(digitalSpecimen.getOdsCreated()).isEqualTo(digitalSpecimenRecord.created());
  }

}
