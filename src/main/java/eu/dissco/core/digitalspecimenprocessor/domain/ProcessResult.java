package eu.dissco.core.digitalspecimenprocessor.domain;

import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenEvent;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenRecord;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.UpdatedDigitalSpecimenTuple;
import java.util.List;

public record ProcessResult(
    List<DigitalSpecimenRecord> equalSpecimens,
    List<UpdatedDigitalSpecimenTuple> changedSpecimens,
    List<DigitalSpecimenEvent> newSpecimens) {

}
