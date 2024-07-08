package eu.dissco.core.digitalspecimenprocessor.util;

import eu.dissco.core.digitalspecimenprocessor.domain.DigitalSpecimenRecord;
import eu.dissco.core.digitalspecimenprocessor.schema.DigitalSpecimen;
import java.util.Date;

public class DigitalSpecimenUtils {

  public static final String DOI_PREFIX = "https://doi.org/";

  private DigitalSpecimenUtils() {
    // This is a utility class
  }

  public static DigitalSpecimen flattenToDigitalSpecimen(
      DigitalSpecimenRecord digitalSpecimenrecord) {
    var digitalSpecimen = digitalSpecimenrecord.digitalSpecimenWrapper().attributes();
    digitalSpecimen.setId(DOI_PREFIX + digitalSpecimenrecord.id());
    digitalSpecimen.setOdsID(DOI_PREFIX + digitalSpecimenrecord.id());
    digitalSpecimen.setOdsVersion(digitalSpecimenrecord.version());
    digitalSpecimen.setOdsMidsLevel(digitalSpecimenrecord.midsLevel());
    digitalSpecimen.setDctermsCreated(Date.from(digitalSpecimenrecord.created()));
    return digitalSpecimen;
  }
}
