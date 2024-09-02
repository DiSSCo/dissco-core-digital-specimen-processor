package eu.dissco.core.digitalspecimenprocessor.domain;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;

public record UpdatedDigitalSpecimenRecord(DigitalSpecimenRecord digitalSpecimenRecord,
                                           List<String> enrichment,
                                           DigitalSpecimenRecord currentDigitalSpecimen,
                                           JsonNode jsonPatch,
                                           List<DigitalMediaEventWithoutDOI> digitalMediaObjectEvents) {

}
