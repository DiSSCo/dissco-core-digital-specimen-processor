package eu.dissco.core.digitalspecimenprocessor.domain.specimen;

import static java.util.Collections.emptyList;

import java.util.List;

public record SpecimenProcessResult(
    List<DigitalSpecimenRecord> equalDigitalSpecimens,
    List<DigitalSpecimenRecord> updatedDigitalSpecimens,
    List<DigitalSpecimenRecord> newDigitalSpecimens
) {

  public SpecimenProcessResult() {
    this(emptyList(), emptyList(), emptyList());
  }
}
