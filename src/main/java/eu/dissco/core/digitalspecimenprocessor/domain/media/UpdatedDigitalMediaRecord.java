package eu.dissco.core.digitalspecimenprocessor.domain.media;

import java.util.Set;
import tools.jackson.databind.JsonNode;

public record UpdatedDigitalMediaRecord(DigitalMediaRecord digitalMediaRecord, Set<String> masList,
		DigitalMediaRecord currentDigitalMediaRecord, JsonNode jsonPatch) {

}
