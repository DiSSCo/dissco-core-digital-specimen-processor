package eu.dissco.core.digitalspecimenprocessor.domain.media;

import eu.dissco.core.digitalspecimenprocessor.schema.DigitalMedia;
import java.util.List;

public record DigitalMediaRecord(
    String id,
    String accessURI,
    List<String> enrichmentList,
    DigitalMedia attributes
) {

}
