package eu.dissco.core.digitalspecimenprocessor.domain.mas;

import eu.dissco.core.digitalspecimenprocessor.database.jooq.enums.MjrTargetType;

public record MasJobRequest(
    String masId,
    String targetId,
    boolean batching,
    String agentId,
    MjrTargetType targetType
) {

}