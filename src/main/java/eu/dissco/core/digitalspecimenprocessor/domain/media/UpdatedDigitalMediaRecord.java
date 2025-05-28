package eu.dissco.core.digitalspecimenprocessor.domain.media;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;

public record UpdatedDigitalMediaRecord(
    DigitalMediaRecord digitalMediaRecord,
    List<String> automatedAnnotations,
    DigitalMediaRecord currentDigitalMediaRecord,
    JsonNode jsonPatch
){

}
