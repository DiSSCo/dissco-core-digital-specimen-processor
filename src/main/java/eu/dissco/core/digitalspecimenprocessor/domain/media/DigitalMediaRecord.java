package eu.dissco.core.digitalspecimenprocessor.domain.media;

import com.fasterxml.jackson.databind.JsonNode;
import eu.dissco.core.digitalspecimenprocessor.schema.DigitalMedia;
import java.time.Instant;
import java.util.List;

public record DigitalMediaRecord(
    String id,
    String accessURI,
    int version, Instant created, List<String> enrichmentList,
    DigitalMedia attributes,
    JsonNode originalAttributes) {

}
