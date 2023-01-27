package eu.dissco.core.digitalspecimenprocessor.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import eu.dissco.core.digitalspecimenprocessor.domain.DigitalSpecimen;
import eu.dissco.core.digitalspecimenprocessor.domain.MIDSFields;
import java.util.List;
import org.springframework.stereotype.Service;

@Service
public class MidsService {

  public int calculateMids(DigitalSpecimen digitalSpecimen) {
    var attributes = getAttributes(digitalSpecimen);
    if (!compliesTo(MIDSFields.MIDS_1, attributes)) {
      return 0;
    }
    return 1;
  }

  private boolean compliesTo(List<MIDSFields> fields, JsonNode attributes) {
    for (var value : fields) {
      if (attributes.get(value.getTerm()) != null) {
        var data = attributes.get(value.getTerm()).asText();
        if (data == null || data.trim().equals("") || data.equals("null")){
          return false;
        }
      } else {
        return false;
      }
    }
    return true;
  }

  private JsonNode getAttributes(DigitalSpecimen digitalSpecimen) {
    ObjectNode attributes = digitalSpecimen.attributes().deepCopy();
    attributes.put(MIDSFields.PHYSICAL_SPECIMEN_ID.getTerm(), digitalSpecimen.physicalSpecimenId());
    attributes.put(MIDSFields.SPECIMEN_TYPE.getTerm(), digitalSpecimen.type());
    return attributes;
  }
}
