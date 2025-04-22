package eu.dissco.core.digitalspecimenprocessor.domain.media;

import java.util.List;

public record UpdatedDigitalMediaTuple(
    DigitalMediaRecord currentDigitalMediaRecord,
    DigitalMediaEvent digitalMediaEvent,
    List<String> newRelatedSpecimenDois) {

}
