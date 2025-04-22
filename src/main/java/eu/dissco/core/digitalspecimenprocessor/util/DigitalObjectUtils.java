package eu.dissco.core.digitalspecimenprocessor.util;

import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaRecord;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenRecord;
import eu.dissco.core.digitalspecimenprocessor.schema.DigitalMedia;
import eu.dissco.core.digitalspecimenprocessor.schema.DigitalSpecimen;
import java.util.Date;

public class DigitalObjectUtils {

  public static final String DOI_PREFIX = "https://doi.org/";

  private DigitalObjectUtils() {
    // This is a utility class
  }

  public static DigitalSpecimen flattenToDigitalSpecimen(
      DigitalSpecimenRecord digitalSpecimenrecord) {
    var digitalSpecimen = digitalSpecimenrecord.digitalSpecimenWrapper().attributes();
    digitalSpecimen.setId(DOI_PREFIX + digitalSpecimenrecord.id());
    digitalSpecimen.setDctermsIdentifier(DOI_PREFIX + digitalSpecimenrecord.id());
    digitalSpecimen.setOdsVersion(digitalSpecimenrecord.version());
    digitalSpecimen.setOdsMidsLevel(digitalSpecimenrecord.midsLevel());
    digitalSpecimen.setDctermsCreated(Date.from(digitalSpecimenrecord.created()));
    return digitalSpecimen;
  }

  public static DigitalMedia flattenToDigitalMedia(DigitalMediaRecord digitalMediaRecord) {
    var digitalMedia = digitalMediaRecord.attributes();
    digitalMedia.setId(DOI_PREFIX + digitalMediaRecord.id());
    digitalMedia.setDctermsIdentifier(DOI_PREFIX + digitalMediaRecord.id());
    // Todo - verify these are correct
    //digitalMedia.setOdsVersion(digitalMediaRecord.version());
    //digitalMedia.setDctermsCreated(Date.from(digitalMediaRecord.created()));
    return digitalMedia;
  }
}
