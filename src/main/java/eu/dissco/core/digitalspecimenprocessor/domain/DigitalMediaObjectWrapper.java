package eu.dissco.core.digitalspecimenprocessor.domain;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;


public record DigitalMediaObjectWrapper(
    @JsonProperty("ods:type")
    String type,
    @JsonProperty("ods:digitalSpecimenId")
    String digitalSpecimenId,
    @JsonProperty("ods:attributes")
    eu.dissco.core.digitalspecimenprocessor.schema.DigitalEntity attributes,
    @JsonProperty("ods:originalAttributes")
    JsonNode originalAttributes) {

}
