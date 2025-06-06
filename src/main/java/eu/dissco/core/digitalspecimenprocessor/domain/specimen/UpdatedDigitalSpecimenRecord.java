package eu.dissco.core.digitalspecimenprocessor.domain.specimen;

import com.fasterxml.jackson.databind.JsonNode;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaEvent;
import eu.dissco.core.digitalspecimenprocessor.domain.relation.MediaRelationshipProcessResult;
import java.util.List;

public record UpdatedDigitalSpecimenRecord(
    DigitalSpecimenRecord digitalSpecimenRecord,
    List<String> enrichment,
    DigitalSpecimenRecord currentDigitalSpecimen,
    JsonNode jsonPatch,
    List<DigitalMediaEvent> digitalMediaObjectEvents,
    MediaRelationshipProcessResult mediaRelationshipProcessResult) {

}
