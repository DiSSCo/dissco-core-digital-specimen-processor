package eu.dissco.core.digitalspecimenprocessor.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import eu.dissco.core.digitalspecimenprocessor.domain.DigitalSpecimen;
import eu.dissco.core.digitalspecimenprocessor.domain.MIDSFields;
import java.util.Collections;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class MidsService {

  private static final String MISSING_MESSAGE = "Field does not comply to field: {}";

  public int calculateMids(DigitalSpecimen digitalSpecimen) {
    var attributes = getAttributes(digitalSpecimen);
    if (doesNotComplyTo(MIDSFields.MIDS_1, attributes)) {
      return 0;
    }
    var midsFields = determineMIDSType(digitalSpecimen);
    if (midsFields.isEmpty() || doesNotComplyTo(midsFields, attributes)) {
      return 1;
    }
    return 2;
  }

  private List<MIDSFields> determineMIDSType(DigitalSpecimen digitalSpecimen) {
    var bioType = List.of("BotanySpecimen", "MycologySpecimen", "MicrobiologyMicroOrganismSpecimen",
        "ZoologyVertebrateSpecimen", "ZoologyInvertebrateSpecimen");
    var paleoType = List.of("PalaeontologySpecimen");
    var geoType = List.of("GeologyRockSpecimen", "GeologyMineralSpecimen",
        "GeologyMixedSolidMatterSpecimen", "AstronomySpecimen");
    if (bioType.contains(digitalSpecimen.type())) {
      return MIDSFields.MIDS_2_BIO;
    } else if (paleoType.contains(digitalSpecimen.type()) || geoType.contains(
        digitalSpecimen.type())) {
      return MIDSFields.MIDS_2_GEO_PALEO;
    } else {
      log.warn("Digital Specimen has unknown type: {} level 1 is highest achievable",
          digitalSpecimen.type());
      return Collections.emptyList();
    }
  }

  private boolean doesNotComplyTo(List<MIDSFields> fields, JsonNode attributes) {
    for (var field : fields) {
      if (field.equals(MIDSFields.QUANTITATIVE_LOCATION)) {
        if (hasInvalidField(attributes, field, null)) {
          log.debug(MISSING_MESSAGE, field);
          return true;
        }
      } else if (field.equals(MIDSFields.HAS_MEDIA)) {
        if (hasInvalidField(attributes, field, "true")) {
          log.debug(MISSING_MESSAGE, field);
          return true;
        }
      } else {
        if (isFieldMissing(attributes, field)) {
          log.debug(MISSING_MESSAGE, field);
          return true;
        }
      }
    }
    return false;
  }

  private boolean isFieldMissing(JsonNode attributes, MIDSFields field) {
    for (var term : field.getTerm()) {
      if (attributes.get(term) != null) {
        var data = attributes.get(term).asText();
        if (data != null && !data.trim().equals("") && !data.equals("null")) {
          return false;
        }
      }
    }
    return true;
  }

  private boolean hasInvalidField(JsonNode attributes, MIDSFields field, String expectedValue) {
    for (var term : field.getTerm()) {
      if (attributes.get(term) != null) {
        var data = attributes.get(term).asText();
        if (data == null || data.trim().equals("") || data.equals("null") ||
            (expectedValue != null && !data.equalsIgnoreCase(expectedValue))) {
          return true;
        }
      } else {
        return true;
      }
    }
    return false;
  }

  private JsonNode getAttributes(DigitalSpecimen digitalSpecimen) {
    ObjectNode attributes = digitalSpecimen.attributes().deepCopy();
    attributes.put(MIDSFields.PHYSICAL_SPECIMEN_ID.getTerm().get(0),
        digitalSpecimen.physicalSpecimenId());
    attributes.put(MIDSFields.SPECIMEN_TYPE.getTerm().get(0), digitalSpecimen.type());
    return attributes;
  }
}
