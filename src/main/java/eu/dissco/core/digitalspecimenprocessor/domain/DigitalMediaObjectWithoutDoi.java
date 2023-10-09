package eu.dissco.core.digitalspecimenprocessor.domain;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;


public record DigitalMediaObjectWithoutDoi(
    @JsonProperty("dcterms:type")
    String type,
    @JsonProperty("ods:physicalSpecimenId")
    String physicalSpecimenId,
    @JsonProperty("ods:attributes")
    eu.dissco.core.digitalspecimenprocessor.schema.DigitalEntity attributes,
    @JsonProperty("ods:originalAttributes")
    JsonNode originalAttributes) {

}
