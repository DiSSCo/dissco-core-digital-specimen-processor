package eu.dissco.core.digitalspecimenprocessor.domain.specimen;

import com.fasterxml.jackson.databind.JsonNode;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaEvent;
import eu.dissco.core.digitalspecimenprocessor.domain.relation.MediaRelationshipProcessResult;
import java.util.List;
import java.util.Set;

public record UpdatedDigitalSpecimenRecord(
    DigitalSpecimenRecord digitalSpecimenRecord,
    Set<String> masList,
    DigitalSpecimenRecord currentDigitalSpecimen,
    JsonNode jsonPatch,
    List<DigitalMediaEvent> digitalMediaObjectEvents,
    MediaRelationshipProcessResult mediaRelationshipProcessResult,
    Boolean isDataFromSourceSystem) {

}
