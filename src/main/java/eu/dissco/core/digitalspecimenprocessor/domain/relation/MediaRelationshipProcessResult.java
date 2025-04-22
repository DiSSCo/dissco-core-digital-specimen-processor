package eu.dissco.core.digitalspecimenprocessor.domain.relation;

import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaEventWithoutDOI;
import eu.dissco.core.digitalspecimenprocessor.schema.EntityRelationship;
import java.util.List;

public record MediaRelationshipProcessResult(
    List<EntityRelationship> tombstonedRelationships,
    List<DigitalMediaEventWithoutDOI> newLinkedObjects
) {

}
