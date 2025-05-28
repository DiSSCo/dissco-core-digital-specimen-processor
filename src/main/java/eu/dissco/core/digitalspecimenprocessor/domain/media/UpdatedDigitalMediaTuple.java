package eu.dissco.core.digitalspecimenprocessor.domain.media;

import java.util.Set;

public record UpdatedDigitalMediaTuple(
    DigitalMediaRecord currentDigitalMediaRecord,
    DigitalMediaEvent digitalMediaEvent,
    Set<String> newRelatedSpecimenDois) {

}
