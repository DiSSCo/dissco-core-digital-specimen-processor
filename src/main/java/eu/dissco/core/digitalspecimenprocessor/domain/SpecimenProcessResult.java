package eu.dissco.core.digitalspecimenprocessor.domain;

import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenEvent;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenRecord;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.UpdatedDigitalSpecimenTuple;
import java.util.List;
import java.util.Map;

public record SpecimenProcessResult(
    List<DigitalSpecimenRecord> equalSpecimens,
    List<UpdatedDigitalSpecimenTuple> changedSpecimens,
    List<DigitalSpecimenEvent> newSpecimens,
    Map<String,String> newSpecimenPids) {

}
