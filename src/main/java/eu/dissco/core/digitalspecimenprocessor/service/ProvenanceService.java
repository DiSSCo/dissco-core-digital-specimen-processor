package eu.dissco.core.digitalspecimenprocessor.service;

import static eu.dissco.core.digitalspecimenprocessor.domain.AgentRoleType.PROCESSING_SERVICE;
import static eu.dissco.core.digitalspecimenprocessor.domain.AgentRoleType.SOURCE_SYSTEM;
import static eu.dissco.core.digitalspecimenprocessor.schema.Agent.Type.PROV_SOFTWARE_AGENT;
import static eu.dissco.core.digitalspecimenprocessor.schema.Identifier.DctermsType.DOI;
import static eu.dissco.core.digitalspecimenprocessor.schema.Identifier.DctermsType.HANDLE;
import static eu.dissco.core.digitalspecimenprocessor.util.AgentUtils.createMachineAgent;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenRecord;
import eu.dissco.core.digitalspecimenprocessor.property.ApplicationProperties;
import eu.dissco.core.digitalspecimenprocessor.schema.CreateUpdateTombstoneEvent;
import eu.dissco.core.digitalspecimenprocessor.schema.DigitalSpecimen;
import eu.dissco.core.digitalspecimenprocessor.schema.OdsChangeValue;
import eu.dissco.core.digitalspecimenprocessor.schema.ProvActivity;
import eu.dissco.core.digitalspecimenprocessor.schema.ProvEntity;
import eu.dissco.core.digitalspecimenprocessor.schema.ProvValue;
import eu.dissco.core.digitalspecimenprocessor.schema.ProvWasAssociatedWith;
import eu.dissco.core.digitalspecimenprocessor.schema.ProvWasAssociatedWith.ProvHadRole;
import eu.dissco.core.digitalspecimenprocessor.util.DigitalSpecimenUtils;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class ProvenanceService {

  private final ObjectMapper mapper;
  private final ApplicationProperties properties;

  public CreateUpdateTombstoneEvent generateCreateEvent(
      DigitalSpecimenRecord digitalSpecimenRecord) {
    var digitalSpecimen = DigitalSpecimenUtils.flattenToDigitalSpecimen(digitalSpecimenRecord);
    return generateCreateUpdateTombStoneEvent(digitalSpecimen, ProvActivity.Type.ODS_CREATE,
        null);
  }

  private CreateUpdateTombstoneEvent generateCreateUpdateTombStoneEvent(
      DigitalSpecimen digitalSpecimen, ProvActivity.Type activityType,
      JsonNode jsonPatch) {
    var entityID = digitalSpecimen.getDctermsIdentifier() + "/" + digitalSpecimen.getOdsVersion();
    var activityID = UUID.randomUUID().toString();
    var sourceSystemID = digitalSpecimen.getOdsSourceSystemID();
    return new CreateUpdateTombstoneEvent()
        .withId(entityID)
        .withType("ods:CreateUpdateTombstoneEvent")
        .withDctermsIdentifier(entityID)
        .withOdsFdoType(properties.getCreateUpdateTombstoneEventType())
        .withProvActivity(new ProvActivity()
            .withId(activityID)
            .withType(activityType)
            .withOdsChangeValue(mapJsonPatch(jsonPatch))
            .withProvEndedAtTime(Date.from(Instant.now()))
            .withProvWasAssociatedWith(List.of(
                new ProvWasAssociatedWith()
                    .withId(sourceSystemID)
                    .withProvHadRole(ProvHadRole.REQUESTOR),
                new ProvWasAssociatedWith()
                    .withId(properties.getPid())
                    .withProvHadRole(ProvHadRole.APPROVER),
                new ProvWasAssociatedWith()
                    .withId(properties.getPid())
                    .withProvHadRole(ProvHadRole.GENERATOR)))
            .withProvUsed(entityID)
            .withRdfsComment(determineComment(activityType)))
        .withProvEntity(new ProvEntity()
            .withId(entityID)
            .withType("ods:DigitalSpecimen")
            .withProvValue(mapEntityToProvValue(digitalSpecimen))
            .withProvWasGeneratedBy(activityID))
        .withOdsHasAgents(List.of(
            createMachineAgent(digitalSpecimen.getOdsSourceSystemName(), sourceSystemID,
                SOURCE_SYSTEM, HANDLE, PROV_SOFTWARE_AGENT),
            createMachineAgent(properties.getName(), properties.getPid(), PROCESSING_SERVICE,
                DOI, PROV_SOFTWARE_AGENT)));
  }

  private String determineComment(ProvActivity.Type activityType) {
    if (activityType == ProvActivity.Type.ODS_CREATE) {
      return "Digital Specimen created";
    } else if (activityType == ProvActivity.Type.ODS_UPDATE) {
      return "Digital Specimen updated";
    } else {
      return null;
    }
  }

  private List<OdsChangeValue> mapJsonPatch(JsonNode jsonPatch) {
    if (jsonPatch == null) {
      return null;
    }
    return mapper.convertValue(jsonPatch, new TypeReference<>() {
    });
  }

  public CreateUpdateTombstoneEvent generateUpdateEvent(DigitalSpecimenRecord digitalSpecimenRecord,
      JsonNode jsonPatch) {
    var digitalSpecimen = DigitalSpecimenUtils.flattenToDigitalSpecimen(digitalSpecimenRecord);
    return generateCreateUpdateTombStoneEvent(digitalSpecimen, ProvActivity.Type.ODS_UPDATE,
        jsonPatch);
  }

  private ProvValue mapEntityToProvValue(DigitalSpecimen digitalSpecimen) {
    var provValue = new ProvValue();
    var node = mapper.convertValue(digitalSpecimen, new TypeReference<Map<String, Object>>() {
    });
    for (var entry : node.entrySet()) {
      provValue.setAdditionalProperty(entry.getKey(), entry.getValue());
    }
    return provValue;
  }
}
