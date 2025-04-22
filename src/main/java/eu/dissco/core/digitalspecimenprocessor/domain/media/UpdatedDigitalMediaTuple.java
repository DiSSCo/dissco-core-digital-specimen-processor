package eu.dissco.core.digitalspecimenprocessor.domain.media;

public record UpdatedDigitalMediaTuple(
    DigitalMediaRecord currentDigitalMediaRecord,
    DigitalMediaEventWithoutDOI digitalMediaEvent
) {

}
