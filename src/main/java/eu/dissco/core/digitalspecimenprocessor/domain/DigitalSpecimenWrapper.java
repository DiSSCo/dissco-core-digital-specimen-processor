package eu.dissco.core.digitalspecimenprocessor.domain;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import eu.dissco.core.digitalspecimenprocessor.schema.DigitalSpecimen;

public record DigitalSpecimenWrapper(
    @JsonProperty("ods:normalisedPhysicalSpecimenID")
    String physicalSpecimenID,
    @JsonProperty("ods:type")
    String type,
    @JsonProperty("ods:attributes")
    DigitalSpecimen attributes,
    @JsonProperty("ods:originalAttributes")
    JsonNode originalAttributes) {

}
