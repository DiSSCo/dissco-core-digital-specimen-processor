package eu.dissco.core.digitalspecimenprocessor.domain.relation;

import java.util.Set;

public record PidProcessResult(
    String doi,
    Set<String> relatedDois
){


}
