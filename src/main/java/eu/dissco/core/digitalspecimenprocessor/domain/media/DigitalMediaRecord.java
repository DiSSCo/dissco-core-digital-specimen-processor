package eu.dissco.core.digitalspecimenprocessor.domain.media;

import eu.dissco.core.digitalspecimenprocessor.schema.DigitalMedia;

public record DigitalMediaRecord(
    String id,
    String accessURI,
    DigitalMedia attributes
) {

}
