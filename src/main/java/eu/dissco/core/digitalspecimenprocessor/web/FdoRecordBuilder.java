package eu.dissco.core.digitalspecimenprocessor.web;

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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class FdoRecordBuilder {

  private final ObjectMapper mapper;
  private static final String DWC_TYPE_STATUS = "dwc:typeStatus";
  private static final Set<String> NOT_TYPE_STATUS = new HashSet<>(
      Arrays.asList("false", "specimen", "type", ""));

  private static final HashMap<String, String> odsMap;

  static {
    HashMap<String, String> map = new HashMap<>();
    map.put("ods:specimenName", REFERENT_NAME.getAttribute());
    map.put("ods:organisationName", SPECIMEN_HOST_NAME.getAttribute());
    map.put("ods:topicDiscipline", TOPIC_DISCIPLINE.getAttribute());
    map.put("ods:physicalSpecimenIdType",
        FdoProfileAttributes.PRIMARY_SPECIMEN_OBJECT_ID_TYPE.getAttribute());
    odsMap = map;
  }

  public List<JsonNode> buildPostHandleRequest(List<DigitalSpecimen> digitalSpecimens)
      throws PidCreationException {
    List<JsonNode> requestBody = new ArrayList<>();
    for (var specimen : digitalSpecimens) {
      requestBody.add(buildSinglePostHandleRequest(specimen));
    }
    return requestBody;
  }

  public List<JsonNode> buildRollbackUpdateRequest(List<DigitalSpecimenRecord> digitalSpecimenRecords)
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
    var organisationId = getTerm(specimen, "ods:organisationId");
    organisationId.ifPresent(orgId -> attributes.put(SPECIMEN_HOST.getAttribute(), orgId));
    if (organisationId.isEmpty()) {
      throw new PidCreationException(
          "Digital Specimen missing ods:organisationId. Unable to create PID. Check specimen"
              + specimen.physicalSpecimenId());
    }

    // Optional
    odsMap.forEach(
        (odsTerm, fdoAttribute) -> updateOptionalAttribute(specimen, odsTerm, fdoAttribute,
            attributes));

    //Must be lower case
    var livingOrPreserved = getTerm(specimen, "ods:livingOrPreserved");
    livingOrPreserved.ifPresent(
        foundTerm -> attributes.put(FdoProfileAttributes.LIVING_OR_PRESERVED.getAttribute(),
            foundTerm.toLowerCase()));

    setMarkedAsType(specimen, attributes);

    return attributes;
  }

  private void setMarkedAsType(DigitalSpecimen specimen, ObjectNode attributeNode) {
    // If typeStatus is present and NOT ["false", "specimen", "type"], this is to true, otherwise left blank.
    var markedAsType = getTerm(specimen, DWC_TYPE_STATUS);
    markedAsType.ifPresent(
        s -> attributeNode.put(FdoProfileAttributes.MARKED_AS_TYPE.getAttribute(),
            !NOT_TYPE_STATUS.contains(s)));
  }

  public boolean handleNeedsUpdate(DigitalSpecimen currentDigitalSpecimen,
      DigitalSpecimen digitalSpecimen) {
    return !currentDigitalSpecimen.physicalSpecimenId().equals(digitalSpecimen.physicalSpecimenId())
        || isUnEqual(currentDigitalSpecimen, digitalSpecimen, "ods:organisationId")
        || isUnEqual(currentDigitalSpecimen, digitalSpecimen, "ods:organisationName")
        || isUnEqual(currentDigitalSpecimen, digitalSpecimen, "ods:specimenName")
        || isUnEqual(currentDigitalSpecimen, digitalSpecimen, "ods:topicDiscipline")
        || isUnEqual(currentDigitalSpecimen, digitalSpecimen, "ods:physicalSpecimenIdType")
        || isUnEqual(currentDigitalSpecimen, digitalSpecimen, "ods:livingOrPreserved")
        || isUnEqual(currentDigitalSpecimen, digitalSpecimen, DWC_TYPE_STATUS);
  }

  private boolean isUnEqual(DigitalSpecimen currentDigitalSpecimen, DigitalSpecimen digitalSpecimen,
      String fieldName) {
    return !Objects.equals(getValueFromAttributes(currentDigitalSpecimen, fieldName),
        getValueFromAttributes(digitalSpecimen, fieldName));
  }

  private String getValueFromAttributes(DigitalSpecimen digitalSpecimen, String fieldName) {
    if (digitalSpecimen.attributes().get(fieldName) != null) {
      return digitalSpecimen.attributes().get(fieldName).asText();
    } else {
      return null;
    }
  }

  private void updateOptionalAttribute(DigitalSpecimen specimen, String term, String fdoAttribute,
      ObjectNode attributeNode) {
    var optionalAttribute = getTerm(specimen, term);
    optionalAttribute.ifPresent(foundTerm -> attributeNode.put(fdoAttribute, foundTerm));
  }

  private Optional<String> getTerm(DigitalSpecimen specimen, String term) {
    var val = specimen.attributes().get(term);
    return jsonNodeIsNull(val) ? Optional.empty() : Optional.of(val.asText());
  }

  private boolean jsonNodeIsNull(JsonNode val) {
    return val == null || val.isNull();
  }

}
