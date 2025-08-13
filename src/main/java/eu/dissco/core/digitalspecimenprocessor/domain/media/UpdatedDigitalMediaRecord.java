package eu.dissco.core.digitalspecimenprocessor.domain.media;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Set;

public record UpdatedDigitalMediaRecord(
    DigitalMediaRecord digitalMediaRecord,
    Set<String> masList,
    DigitalMediaRecord currentDigitalMediaRecord,
    JsonNode jsonPatch
){

}
