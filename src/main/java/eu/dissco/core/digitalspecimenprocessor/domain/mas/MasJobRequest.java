package eu.dissco.core.digitalspecimenprocessor.domain.mas;

public record MasJobRequest(
    String masId,
    String targetId,
    boolean batching,
    String agentId,
    MjrTargetType targetType
) {

}