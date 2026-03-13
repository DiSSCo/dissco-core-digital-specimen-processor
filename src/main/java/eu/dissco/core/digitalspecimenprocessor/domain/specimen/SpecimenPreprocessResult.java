package eu.dissco.core.digitalspecimenprocessor.domain.specimen;

import java.util.List;
import java.util.Map;

public record SpecimenPreprocessResult(
    Map<DigitalSpecimenRecord, DigitalSpecimenEvent> equalSpecimens,
    List<UpdatedDigitalSpecimenTuple> changedSpecimens,
    List<DigitalSpecimenEvent> newSpecimens,
    Map<String,String> newSpecimenPids) {

}
