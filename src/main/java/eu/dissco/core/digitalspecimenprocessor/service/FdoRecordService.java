package eu.dissco.core.digitalspecimenprocessor.service;

import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.ISSUED_FOR_AGENT;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.LIVING_OR_PRESERVED;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.MARKED_AS_TYPE;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.PRIMARY_MEDIA_ID;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.REFERENT_NAME;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.SPECIMEN_HOST;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.SPECIMEN_HOST_NAME;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.TOPIC_DISCIPLINE;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaEventWithoutDOI;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenRecord;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenWrapper;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.UpdatedDigitalSpecimenTuple;
import eu.dissco.core.digitalspecimenprocessor.property.FdoProperties;
import java.util.ArrayList;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class FdoRecordService {

  private static final String ATTRIBUTES = "attributes";
  private static final String TYPE = "type";
  private static final String ID = "id";
  private static final String DATA = "data";
  private final ObjectMapper mapper;
  private final FdoProperties fdoProperties;

  private static boolean isEqualString(String currentValue, String newValue) {
    return currentValue != null && !currentValue.equals(newValue);
  }

  public List<JsonNode> buildPostHandleRequest(List<DigitalSpecimenWrapper> digitalSpecimens) {
    return digitalSpecimens.stream().map(this::buildSinglePostHandleRequest).toList();
  }

  public List<JsonNode> buildPostRequestMedia(String specimenId, List<DigitalMediaEventWithoutDOI> digitalMedia) {
    var requests = new ArrayList<JsonNode>();
    for (var media : digitalMedia) {
      requests.add(
          mapper.createObjectNode().set("data",
              mapper.createObjectNode()
                  .put("type", fdoProperties.getMediaType())
                  .set(ATTRIBUTES, mapper.createObjectNode()
                      .put(ISSUED_FOR_AGENT.getAttribute(), fdoProperties.getIssuedForAgent())
                      .put("mediaHost", media.digitalMediaObjectWithoutDoi().attributes().getOdsOrganisationID())
                      .put("isDerivedFromSpecimen", true)
                      .put("linkedDigitalObjectType", "digital specimen")
                      .put("linkedDigitalObjectPid", specimenId)
                      .put("licenseUrl",
                          media.digitalMediaObjectWithoutDoi().attributes().getDctermsLicense())
                      .put(PRIMARY_MEDIA_ID.getAttribute(), media.digitalMediaObjectWithoutDoi().attributes().getAcAccessURI()))
          ));
    }
    return requests;
  }

  public List<JsonNode> buildUpdateHandleRequest(
      List<UpdatedDigitalSpecimenTuple> digitalSpecimens) {
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

  public List<String> buildRollbackCreationRequest(List<DigitalSpecimenRecord> digitalSpecimens) {
    return digitalSpecimens.stream().map(DigitalSpecimenRecord::id).toList();
  }

  private JsonNode buildSinglePostHandleRequest(DigitalSpecimenWrapper specimen) {
    return mapper.createObjectNode()
        .set(DATA, mapper.createObjectNode()
            .put(TYPE, fdoProperties.getSpecimenType())
            .set(ATTRIBUTES, genRequestAttributes(specimen)));
  }

  private JsonNode buildSingleUpdateHandleRequest(UpdatedDigitalSpecimenTuple specimenTuple) {
    var request = mapper.createObjectNode();
    var data = mapper.createObjectNode();
    data.put(ID, specimenTuple.currentSpecimen().id());
    data.put(TYPE, fdoProperties.getSpecimenType());
    var attributes = genRequestAttributes(
        specimenTuple.digitalSpecimenEvent().digitalSpecimenWrapper());
    data.set(ATTRIBUTES, attributes);
    request.set(DATA, data);
    return request;
  }

  private JsonNode buildSingleRollbackUpdateRequest(DigitalSpecimenRecord specimen) {
    var request = mapper.createObjectNode();
    var data = mapper.createObjectNode();
    data.put(TYPE, fdoProperties.getSpecimenType());
    var attributes = genRequestAttributes(specimen.digitalSpecimenWrapper());
    data.put(ID, specimen.id());
    data.set(ATTRIBUTES, attributes);
    request.set(DATA, data);
    return request;
  }

  private JsonNode genRequestAttributes(DigitalSpecimenWrapper specimen) {
    var attributes = mapper.createObjectNode()
        .put(FdoProfileAttributes.ISSUED_FOR_AGENT.getAttribute(),
            fdoProperties.getIssuedForAgent())
        .put(FdoProfileAttributes.PRIMARY_SPECIMEN_OBJECT_ID.getAttribute(),
            specimen.physicalSpecimenID())
        .put(FdoProfileAttributes.NORMALISED_PRIMARY_SPECIMEN_OBJECT_ID.getAttribute(),
            specimen.attributes().getOdsNormalisedPhysicalSpecimenID())
        .put(FdoProfileAttributes.PRIMARY_SPECIMEN_OBJECT_ID_TYPE.getAttribute(),
            specimen.attributes().getOdsPhysicalSpecimenIDType().value())
        .put(SPECIMEN_HOST.getAttribute(), specimen.attributes().getOdsOrganisationID());
    // Optional
    addOptionalAttributes(specimen, attributes);
    return attributes;
  }

  private void addOptionalAttributes(DigitalSpecimenWrapper specimen, ObjectNode attributes) {
    if (specimen.attributes().getOdsOrganisationName() != null) {
      attributes.put(SPECIMEN_HOST_NAME.getAttribute(),
          specimen.attributes().getOdsOrganisationName());
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
    if (specimen.attributes().getOdsIsMarkedAsType() != null) {
      attributes.put(MARKED_AS_TYPE.getAttribute(), specimen.attributes().getOdsIsMarkedAsType());
    }
    if (specimen.attributes().getOdsHasIdentifier() != null && !specimen.attributes()
        .getOdsHasIdentifier().isEmpty()) {
      var idNodeArr = mapper.createArrayNode();
      for (var id : specimen.attributes().getOdsHasIdentifier()) {
        idNodeArr.add(mapper.createObjectNode()
            .put("identifierType", id.getDctermsTitle())
            .put("identifierValue", id.getDctermsIdentifier()));
      }
      attributes.set("otherSpecimenIds", idNodeArr);
    }
  }

  public boolean handleNeedsUpdate(DigitalSpecimenWrapper currentDigitalSpecimenWrapper,
      DigitalSpecimenWrapper digitalSpecimenWrapper) {
    var currentAttributes = currentDigitalSpecimenWrapper.attributes();
    var attributes = digitalSpecimenWrapper.attributes();
    return isEqualString(currentDigitalSpecimenWrapper.physicalSpecimenID(),
        digitalSpecimenWrapper.physicalSpecimenID())
        || isEqualString(
        currentDigitalSpecimenWrapper.attributes().getOdsNormalisedPhysicalSpecimenID(),
        digitalSpecimenWrapper.attributes().getOdsNormalisedPhysicalSpecimenID())
        || isEqualString(currentAttributes.getOdsOrganisationID(),
        attributes.getOdsOrganisationID())
        || isEqualString(currentAttributes.getOdsOrganisationName(),
        attributes.getOdsOrganisationName())
        || (currentAttributes.getOdsTopicDiscipline() != null
        && !currentAttributes.getOdsTopicDiscipline().equals(attributes.getOdsTopicDiscipline()))
        || (currentAttributes.getOdsPhysicalSpecimenIDType() != null
        && !currentAttributes.getOdsPhysicalSpecimenIDType()
        .equals(attributes.getOdsPhysicalSpecimenIDType()))
        || (currentAttributes.getOdsLivingOrPreserved() != null && isEqualString(
        currentAttributes.getOdsLivingOrPreserved().value(),
        attributes.getOdsLivingOrPreserved().value()))
        || isEqualString(currentAttributes.getOdsSpecimenName(), attributes.getOdsSpecimenName())
        || (currentAttributes.getOdsIsMarkedAsType() != null
        && !currentAttributes.getOdsIsMarkedAsType().equals(attributes.getOdsIsMarkedAsType()));
  }
}
