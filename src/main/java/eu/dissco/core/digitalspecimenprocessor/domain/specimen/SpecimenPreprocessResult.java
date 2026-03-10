package eu.dissco.core.digitalspecimenprocessor.domain.specimen;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;
import java.util.Map;

public record SpecimenPreprocessResult(
    Map<DigitalSpecimenRecord, JsonNode> equalSpecimens,
    List<UpdatedDigitalSpecimenTuple> changedSpecimens,
    List<DigitalSpecimenEvent> newSpecimens,
    Map<String,String> newSpecimenPids) {

}
