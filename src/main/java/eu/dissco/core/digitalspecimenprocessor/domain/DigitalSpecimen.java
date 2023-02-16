package eu.dissco.core.digitalspecimenprocessor.domain;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;

public record DigitalSpecimen(
    @JsonProperty("ods:physicalSpecimenId")
    String physicalSpecimenId,
    @JsonProperty("ods:type")
    String type,
    @JsonProperty("ods:attributes")
    JsonNode attributes,
    @JsonProperty("ods:originalAttributes")
    JsonNode originalAttributes) {

}
