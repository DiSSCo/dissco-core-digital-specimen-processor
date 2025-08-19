package eu.dissco.core.digitalspecimenprocessor.domain.specimen;

import java.util.List;

public record SpecimenProcessResult(
    List<DigitalSpecimenRecord> equalDigitalSpecimens,
    List<DigitalSpecimenRecord> updatedDigitalSpecimens,
    List<DigitalSpecimenRecord> newDigitalSpecimens
) {

}
