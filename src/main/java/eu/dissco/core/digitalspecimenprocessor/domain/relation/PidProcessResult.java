package eu.dissco.core.digitalspecimenprocessor.domain.relation;

import java.util.Set;

public record PidProcessResult(
    String doiOfTarget,
    Set<String> doisOfRelatedObjects
){


}
