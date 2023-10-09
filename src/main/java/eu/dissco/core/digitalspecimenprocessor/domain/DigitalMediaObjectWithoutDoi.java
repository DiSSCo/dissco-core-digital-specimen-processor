package eu.dissco.core.digitalspecimenprocessor.domain;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import eu.dissco.core.digitalspecimenprocessor.schema.DigitalEntity;


public record DigitalMediaObjectWithoutDoi(
    @JsonProperty("ods:type")
    String type,
    @JsonProperty("ods:physicalSpecimenId")
    String physicalSpecimenId,
    @JsonProperty("ods:attributes")
    DigitalEntity attributes,
    @JsonProperty("ods:originalAttributes")
    JsonNode originalAttributes) {

}
