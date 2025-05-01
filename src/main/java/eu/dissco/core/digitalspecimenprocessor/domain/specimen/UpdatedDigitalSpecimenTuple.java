package eu.dissco.core.digitalspecimenprocessor.domain.specimen;

import eu.dissco.core.digitalspecimenprocessor.domain.relation.MediaRelationshipProcessResult;

public record UpdatedDigitalSpecimenTuple(
    DigitalSpecimenRecord currentSpecimen,
    DigitalSpecimenEvent digitalSpecimenEvent,
    MediaRelationshipProcessResult mediaRelationshipProcessResult) {

}
