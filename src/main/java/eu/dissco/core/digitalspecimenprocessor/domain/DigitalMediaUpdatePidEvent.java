package eu.dissco.core.digitalspecimenprocessor.domain;

public record DigitalMediaUpdatePidEvent(
    String digitalSpecimenPID,
    String digitalMediaPID,
    String digitalMediaAccessURI
) {

}
