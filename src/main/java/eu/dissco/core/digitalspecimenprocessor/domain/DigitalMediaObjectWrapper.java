package eu.dissco.core.digitalspecimenprocessor.domain;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import eu.dissco.core.digitalspecimenprocessor.schema.DigitalEntity;


public record DigitalMediaObjectWrapper(
    @JsonProperty("ods:type")
    String type,
    @JsonProperty("ods:digitalSpecimenId")
    String digitalSpecimenId,
    @JsonProperty("ods:attributes")
    DigitalEntity attributes,
    @JsonProperty("ods:originalAttributes")
    JsonNode originalAttributes) {

}
