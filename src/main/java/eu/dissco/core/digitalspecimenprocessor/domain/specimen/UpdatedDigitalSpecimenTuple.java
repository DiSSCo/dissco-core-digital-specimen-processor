package eu.dissco.core.digitalspecimenprocessor.domain.specimen;

import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaProcessResult;

public record UpdatedDigitalSpecimenTuple(
    DigitalSpecimenRecord currentSpecimen,
    DigitalSpecimenEvent digitalSpecimenEvent,
    DigitalMediaProcessResult digitalMediaProcessResult) {

}
