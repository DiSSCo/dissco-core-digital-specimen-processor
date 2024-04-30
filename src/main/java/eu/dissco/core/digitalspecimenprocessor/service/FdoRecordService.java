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
import eu.dissco.core.digitalspecimenprocessor.domain.DigitalSpecimenRecord;
import eu.dissco.core.digitalspecimenprocessor.domain.DigitalSpecimenWrapper;
import eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes;
import eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileConstants;
import eu.dissco.core.digitalspecimenprocessor.domain.UpdatedDigitalSpecimenTuple;
import java.util.ArrayList;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class FdoRecordService {

  private final ObjectMapper mapper;

  private static final String ATTRIBUTES = "attributes";
  private static final String TYPE = "type";
  private static final String ID = "id";

  private static final String DATA = "data";

  private static boolean isEqualString(String currentValue, String newValue) {
    return currentValue != null && !currentValue.equals(newValue);
  }

  public List<JsonNode> buildPostHandleRequest(List<DigitalSpecimenWrapper> digitalSpecimens) {
    return digitalSpecimens.stream().map(this::buildSinglePostHandleRequest).toList();
  }

  public List<JsonNode> buildUpdateHandleRequest(List<UpdatedDigitalSpecimenTuple> digitalSpecimens){
    return digitalSpecimens.stream().map(this::buildSingleUpdateHandleRequest).toList();
  }

  public List<JsonNode> buildRollbackUpdateRequest(
      List<DigitalSpecimenRecord> digitalSpecimenRecords) {
    List<JsonNode> requestBody = new ArrayList<>();
    for (var specimen : digitalSpecimenRecords) {
      requestBody.add(buildSingleRollbackUpdateRequest(specimen));
    }
    return requestBody;
  }

  public JsonNode buildRollbackCreationRequest(List<DigitalSpecimenRecord> digitalSpecimens) {
    var handles = digitalSpecimens.stream().map(DigitalSpecimenRecord::id).toList();
    var dataNode = handles.stream()
        .map(handle -> mapper.createObjectNode().put(ID, handle))
        .toList();
    ArrayNode dataArray = mapper.valueToTree(dataNode);
    return mapper.createObjectNode().set(DATA, dataArray);
  }

  private JsonNode buildSinglePostHandleRequest(DigitalSpecimenWrapper specimen) {
    var request = mapper.createObjectNode();
    var data = mapper.createObjectNode();
    data.put(TYPE, FdoProfileConstants.DIGITAL_SPECIMEN_TYPE.getValue());
    var attributes = genRequestAttributes(specimen);
    data.set(ATTRIBUTES, attributes);
    request.set(DATA, data);
    return request;
  }

  private JsonNode buildSingleUpdateHandleRequest(UpdatedDigitalSpecimenTuple specimenTuple) {
    var request = mapper.createObjectNode();
    var data = mapper.createObjectNode();
    data.put(ID, specimenTuple.currentSpecimen().id());
    data.put(TYPE, FdoProfileConstants.DIGITAL_SPECIMEN_TYPE.getValue());
    var attributes = genRequestAttributes(specimenTuple.digitalSpecimenEvent().digitalSpecimenWrapper());
    data.set(ATTRIBUTES, attributes);
    request.set(DATA, data);
    return request;
  }

  private JsonNode buildSingleRollbackUpdateRequest(DigitalSpecimenRecord specimen) {
    var request = mapper.createObjectNode();
    var data = mapper.createObjectNode();
    data.put(TYPE, FdoProfileConstants.DIGITAL_SPECIMEN_TYPE.getValue());
    var attributes = genRequestAttributes(specimen.digitalSpecimenWrapper());
    data.put(ID, specimen.id());
    data.set(ATTRIBUTES, attributes);
    request.set(DATA, data);
    return request;
  }

  private JsonNode genRequestAttributes(DigitalSpecimenWrapper specimen) {
    var attributes = mapper.createObjectNode();
    // Defaults
    attributes.put(FdoProfileAttributes.FDO_PROFILE.getAttribute(),
        FdoProfileConstants.FDO_PROFILE.getValue());
    attributes.put(FdoProfileAttributes.ISSUED_FOR_AGENT.getAttribute(),
        FdoProfileConstants.ISSUED_FOR_AGENT_PID.getValue());

    // Mandatory
    attributes.put(FdoProfileAttributes.PRIMARY_SPECIMEN_OBJECT_ID.getAttribute(),
        specimen.physicalSpecimenId());
    attributes.put(FdoProfileAttributes.NORMALISED_PRIMARY_SPECIMEN_OBJECT_ID.getAttribute(),
        specimen.attributes().getOdsNormalisedPhysicalSpecimenId());
    attributes.put(FdoProfileAttributes.PRIMARY_SPECIMEN_OBJECT_ID_TYPE.getAttribute(),
        specimen.attributes().getOdsPhysicalSpecimenIdType().value());
    attributes.put(SPECIMEN_HOST.getAttribute(), specimen.attributes().getDwcInstitutionId());

    addOptionalAttributes(specimen, attributes);

    return attributes;
  }

  private void addOptionalAttributes(DigitalSpecimenWrapper specimen, ObjectNode attributes) {
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
          specimen.attributes().getOdsLivingOrPreserved().value());
    }
    if (specimen.attributes().getOdsMarkedAsType() != null) {
      attributes.put(MARKED_AS_TYPE.getAttribute(), specimen.attributes().getOdsMarkedAsType());
    }
    if (specimen.attributes().getIdentifiers() != null && !specimen.attributes().getIdentifiers().isEmpty()){
      var idNodeArr = mapper.createArrayNode();
      for (var id: specimen.attributes().getIdentifiers()){
        idNodeArr.add(mapper.createObjectNode()
            .put("identifierType", id.getIdentifierType())
            .put("identifierValue", id.getIdentifierValue()));
      }
      attributes.set("otherSpecimenIds", idNodeArr);
    }
  }

  public boolean handleNeedsUpdate(DigitalSpecimenWrapper currentDigitalSpecimenWrapper,
      DigitalSpecimenWrapper digitalSpecimenWrapper) {
    var currentAttributes = currentDigitalSpecimenWrapper.attributes();
    var attributes = digitalSpecimenWrapper.attributes();
    return isEqualString(currentDigitalSpecimenWrapper.physicalSpecimenId(),
        digitalSpecimenWrapper.physicalSpecimenId())
        || isEqualString(
        currentDigitalSpecimenWrapper.attributes().getOdsNormalisedPhysicalSpecimenId(),
        digitalSpecimenWrapper.attributes().getOdsNormalisedPhysicalSpecimenId())
        || isEqualString(currentAttributes.getDwcInstitutionId(), attributes.getDwcInstitutionId())
        || isEqualString(currentAttributes.getDwcInstitutionName(),
        attributes.getDwcInstitutionName())
        || (currentAttributes.getOdsTopicDiscipline() != null
        && !currentAttributes.getOdsTopicDiscipline().equals(attributes.getOdsTopicDiscipline()))
        || (currentAttributes.getOdsPhysicalSpecimenIdType() != null
        && !currentAttributes.getOdsPhysicalSpecimenIdType()
        .equals(attributes.getOdsPhysicalSpecimenIdType()))
        || (currentAttributes.getOdsLivingOrPreserved() != null && isEqualString(
        currentAttributes.getOdsLivingOrPreserved().value(),
        attributes.getOdsLivingOrPreserved().value()))
        || isEqualString(currentAttributes.getOdsSpecimenName(), attributes.getOdsSpecimenName())
        || (currentAttributes.getOdsMarkedAsType() != null
        && !currentAttributes.getOdsMarkedAsType().equals(attributes.getOdsMarkedAsType()));
  }
}
