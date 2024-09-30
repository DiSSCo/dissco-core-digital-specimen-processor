package eu.dissco.core.digitalspecimenprocessor.domain.specimen;

import com.fasterxml.jackson.databind.JsonNode;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaEventWithoutDOI;
import java.util.List;

public record UpdatedDigitalSpecimenRecord(DigitalSpecimenRecord digitalSpecimenRecord,
                                           List<String> enrichment,
                                           DigitalSpecimenRecord currentDigitalSpecimen,
                                           JsonNode jsonPatch,
                                           List<DigitalMediaEventWithoutDOI> digitalMediaObjectEvents) {

}
