package eu.dissco.core.digitalspecimenprocessor.util;

import eu.dissco.core.digitalspecimenprocessor.domain.DigitalSpecimenRecord;
import eu.dissco.core.digitalspecimenprocessor.schema.DigitalSpecimen;
import java.util.Date;

public class DigitalSpecimenUtils {

  private DigitalSpecimenUtils() {
    // This is a utility class
  }

  public static DigitalSpecimen flattenToDigitalSpecimen(
      DigitalSpecimenRecord digitalSpecimenrecord) {
    var digitalSpecimen = digitalSpecimenrecord.digitalSpecimenWrapper().attributes();
    digitalSpecimen.setId(digitalSpecimenrecord.id());
    digitalSpecimen.setOdsID(digitalSpecimenrecord.id());
    digitalSpecimen.setOdsVersion(digitalSpecimenrecord.version());
    digitalSpecimen.setOdsMidsLevel(digitalSpecimenrecord.midsLevel());
    digitalSpecimen.setOdsCreated(Date.from(digitalSpecimenrecord.created()));
    return digitalSpecimen;
  }
}
