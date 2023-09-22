package eu.dissco.core.digitalspecimenprocessor.service;

import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.LIVING_OR_PRESERVED;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.MARKED_AS_TYPE;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.REFERENT_NAME;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.SPECIMEN_HOST;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.SPECIMEN_HOST_NAME;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.TOPIC_DISCIPLINE;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import eu.dissco.core.digitalspecimenprocessor.domain.DigitalSpecimen;
import eu.dissco.core.digitalspecimenprocessor.domain.DigitalSpecimenRecord;
import eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes;
import eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileConstants;
import eu.dissco.core.digitalspecimenprocessor.exception.PidCreationException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class FdoRecordService {

  private static final Set<String> NOT_TYPE_STATUS = new HashSet<>(
      Arrays.asList("false", "specimen", "type", ""));

  private final ObjectMapper mapper;

  private static boolean isEqualString(String currentValue, String newValue) {
    return currentValue != null && !currentValue.equals(newValue);
  }

  public List<JsonNode> buildPostHandleRequest(List<DigitalSpecimen> digitalSpecimens)
      throws PidCreationException {
    List<JsonNode> requestBody = new ArrayList<>();
    for (var specimen : digitalSpecimens) {
      requestBody.add(buildSinglePostHandleRequest(specimen));
    }
    return requestBody;
  }

  public List<JsonNode> buildRollbackUpdateRequest(
      List<DigitalSpecimenRecord> digitalSpecimenRecords)
      throws PidCreationException {
    List<JsonNode> requestBody = new ArrayList<>();
    for (var specimen : digitalSpecimenRecords) {
      requestBody.add(buildSingleRollbackUpdateRequest(specimen));
    }
    return requestBody;
  }

  public JsonNode buildRollbackCreationRequest(List<DigitalSpecimenRecord> digitalSpecimens) {
    var handles = digitalSpecimens.stream().map(DigitalSpecimenRecord::id).toList();
    var dataNode = handles.stream()
        .map(handle -> mapper.createObjectNode().put("id", handle))
        .toList();
    ArrayNode dataArray = mapper.valueToTree(dataNode);
    return mapper.createObjectNode().set("data", dataArray);
  }

  private JsonNode buildSinglePostHandleRequest(DigitalSpecimen specimen)
      throws PidCreationException {
    var request = mapper.createObjectNode();
    var data = mapper.createObjectNode();
    data.put("type", FdoProfileConstants.DIGITAL_SPECIMEN_TYPE.getValue());
    var attributes = genRequestAttributes(specimen);
    data.set("attributes", attributes);
    request.set("data", data);
    return request;
  }

  private JsonNode buildSingleRollbackUpdateRequest(DigitalSpecimenRecord specimen)
      throws PidCreationException {
    var request = mapper.createObjectNode();
    var data = mapper.createObjectNode();
    data.put("type", FdoProfileConstants.DIGITAL_SPECIMEN_TYPE.getValue());
    var attributes = genRequestAttributes(specimen.digitalSpecimen());
    data.put("id", specimen.id());
    data.set("attributes", attributes);
    request.set("data", data);
    return request;
  }

  private JsonNode genRequestAttributes(DigitalSpecimen specimen) throws PidCreationException {
    var attributes = mapper.createObjectNode();
    // Defaults
    attributes.put(FdoProfileAttributes.FDO_PROFILE.getAttribute(),
        FdoProfileConstants.FDO_PROFILE.getValue());
    attributes.put(FdoProfileAttributes.DIGITAL_OBJECT_TYPE.getAttribute(),
        FdoProfileConstants.DIGITAL_OBJECT_TYPE.getValue());
    attributes.put(FdoProfileAttributes.ISSUED_FOR_AGENT.getAttribute(),
        FdoProfileConstants.ISSUED_FOR_AGENT_PID.getValue());

    // Mandatory
    attributes.put(FdoProfileAttributes.PRIMARY_SPECIMEN_OBJECT_ID.getAttribute(),
        specimen.physicalSpecimenId());
    attributes.put(FdoProfileAttributes.PRIMARY_SPECIMEN_OBJECT_ID_TYPE.getAttribute(),
        specimen.attributes().getOdsPhysicalSpecimenIdType().value().toLowerCase());
    var organisationId = specimen.attributes().getDwcInstitutionId();
    if (organisationId != null) {
      attributes.put(SPECIMEN_HOST.getAttribute(), organisationId);
    } else {
      throw new PidCreationException(
          "Digital Specimen missing ods:organisationId. Unable to create PID. Check specimen"
              + specimen.physicalSpecimenId());
    }

    addOptionalAttributes(specimen, attributes);

    return attributes;
  }

  private void addOptionalAttributes(DigitalSpecimen specimen, ObjectNode attributes) {
    if (specimen.attributes().getOdsSourceSystem() != null) {
      attributes.put(FdoProfileAttributes.SOURCE_SYSTEM_ID.getAttribute(),
          specimen.attributes().getOdsSourceSystem());
    }
    if (specimen.attributes().getDwcInstitutionName() != null) {
      attributes.put(SPECIMEN_HOST_NAME.getAttribute(),
          specimen.attributes().getDwcInstitutionName());
    }
    if (specimen.attributes().getOdsTopicDiscipline() != null) {
      attributes.put(TOPIC_DISCIPLINE.getAttribute(),
          specimen.attributes().getOdsTopicDiscipline().value());
    }
    if (specimen.attributes().getOdsSpecimenName() != null) {
      attributes.put(REFERENT_NAME.getAttribute(), specimen.attributes().getOdsSpecimenName());
    }
    if (specimen.attributes().getOdsLivingOrPreserved() != null) {
      attributes.put(LIVING_OR_PRESERVED.getAttribute(),
          specimen.attributes().getOdsLivingOrPreserved().toLowerCase());
    }
    var typeStatus = specimen.attributes().getDwcTypeStatus();
    if (typeStatus != null) {
      attributes.put(MARKED_AS_TYPE.getAttribute(), !NOT_TYPE_STATUS.contains(typeStatus));
    }
  }

  public boolean handleNeedsUpdate(DigitalSpecimen currentDigitalSpecimen,
      DigitalSpecimen digitalSpecimen) {
    var currentAttributes = currentDigitalSpecimen.attributes();
    var attributes = digitalSpecimen.attributes();
    return isEqualString(currentDigitalSpecimen.physicalSpecimenId(),
        digitalSpecimen.physicalSpecimenId())
        || isEqualString(currentAttributes.getDwcInstitutionId(), attributes.getDwcInstitutionId())
        || isEqualString(currentAttributes.getDwcInstitutionName(),
        attributes.getDwcInstitutionName())
        || (currentAttributes.getOdsTopicDiscipline() != null
        && !currentAttributes.getOdsTopicDiscipline().equals(attributes.getOdsTopicDiscipline()))
        || (currentAttributes.getOdsPhysicalSpecimenIdType() != null
        && !currentAttributes.getOdsPhysicalSpecimenIdType()
        .equals(attributes.getOdsPhysicalSpecimenIdType()))
        || isEqualString(currentAttributes.getOdsLivingOrPreserved(),
        attributes.getOdsLivingOrPreserved())
        || isEqualString(currentAttributes.getOdsSpecimenName(), attributes.getOdsSpecimenName())
        || isEqualString(currentAttributes.getDwcTypeStatus(), attributes.getDwcTypeStatus());
  }
}
