package eu.dissco.core.digitalspecimenprocessor.domain.specimen;

import static java.util.Collections.emptyList;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;
import java.util.Map;

public record SpecimenProcessResult(
    Map<DigitalSpecimenRecord, JsonNode> equalDigitalSpecimens,
    List<DigitalSpecimenRecord> updatedDigitalSpecimens,
    List<DigitalSpecimenRecord> newDigitalSpecimens
) {

  public SpecimenProcessResult() {
    this(Map.of(), emptyList(), emptyList());
  }
}
