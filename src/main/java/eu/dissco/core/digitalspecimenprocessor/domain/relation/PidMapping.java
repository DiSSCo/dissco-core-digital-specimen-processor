package eu.dissco.core.digitalspecimenprocessor.domain.relation;

import java.util.Map;

public record PidMapping(
    String localId,
    String DOI,
    Map<String, String>
) {

}
