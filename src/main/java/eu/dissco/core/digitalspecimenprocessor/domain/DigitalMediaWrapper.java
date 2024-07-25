package eu.dissco.core.digitalspecimenprocessor.domain;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import eu.dissco.core.digitalspecimenprocessor.schema.DigitalMedia;


public record DigitalMediaWrapper(
    @JsonProperty("ods:type")
    String type,
    @JsonProperty("ods:digitalSpecimenID")
    String digitalSpecimenID,
    @JsonProperty("ods:attributes")
    DigitalMedia attributes,
    @JsonProperty("ods:originalAttributes")
    JsonNode originalAttributes
) {

}
