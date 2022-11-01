package eu.dissco.core.digitalspecimenprocessor.domain;

import java.util.List;

public record ProcessResult(
    List<DigitalSpecimenRecord> equalSpecimens,
    List<UpdatedDigitalSpecimenTuple> changedSpecimens,
    List<DigitalSpecimenEvent> newSpecimens) {

}
