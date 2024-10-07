package eu.dissco.core.digitalspecimenprocessor.domain.media;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import eu.dissco.core.digitalspecimenprocessor.schema.DigitalMedia;


public record DigitalMediaWithoutDOI(
    @JsonProperty("ods:type")
    String type,
    @JsonProperty("ods:physicalSpecimenID")
    String physicalSpecimenID,
    @JsonProperty("ods:attributes")
    DigitalMedia attributes,
    @JsonProperty("ods:originalAttributes")
    JsonNode originalAttributes) {

}
