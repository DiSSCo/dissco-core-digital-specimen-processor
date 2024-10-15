package eu.dissco.core.digitalspecimenprocessor.domain.media;

import eu.dissco.core.digitalspecimenprocessor.schema.EntityRelationship;
import java.util.List;

public record DigitalMediaProcessResult(
    List<EntityRelationship> unchangedMedia,
    List<EntityRelationship> tombstoneMedia,
    List<DigitalMediaEventWithoutDOI> newMedia
) {

}
