package eu.dissco.core.digitalspecimenprocessor.domain.media;

import com.fasterxml.jackson.databind.JsonNode;
import eu.dissco.core.digitalspecimenprocessor.schema.DigitalMedia;
import java.time.Instant;
import java.util.Set;

public record DigitalMediaRecord(
    String id,
    String accessURI,
    int version, Instant created,
    Set<String> masIds,
    DigitalMedia attributes,
    JsonNode originalAttributes,
    Boolean forceMasSchedule
) {

}
