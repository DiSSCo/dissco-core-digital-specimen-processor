package eu.dissco.core.digitalspecimenprocessor.domain.relation;

import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaEvent;
import eu.dissco.core.digitalspecimenprocessor.schema.EntityRelationship;
import java.util.Collections;
import java.util.List;

public record MediaRelationshipProcessResult(
    List<EntityRelationship> tombstonedRelationships,
    List<DigitalMediaEvent> newLinkedObjects,
    List<EntityRelationship> unchangedRelationships
) {

  public MediaRelationshipProcessResult() {
    this(Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
  }
}
