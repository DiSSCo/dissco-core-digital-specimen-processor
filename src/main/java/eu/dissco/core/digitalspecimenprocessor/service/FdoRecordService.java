package eu.dissco.core.digitalspecimenprocessor.service;

import static eu.dissco.core.digitalspecimenprocessor.domain.AgentRoleType.RIGHTS_OWNER;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.CATALOG_NUMBER;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.LICENSE_NAME;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.LICENSE_URL;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.LIVING_OR_PRESERVED;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.MARKED_AS_TYPE;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.MEDIA_HOST;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.MEDIA_HOST_NAME;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.MEDIA_TYPE;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.MIME_TYPE;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.PRIMARY_MEDIA_ID;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.PRIMARY_MEDIA_ID_NAME;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.PRIMARY_MEDIA_ID_TYPE;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.REFERENT_NAME;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.RIGHTS_HOLDER;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.RIGHTS_HOLDER_PID;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.SPECIMEN_HOST;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.SPECIMEN_HOST_NAME;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.TOPIC_DISCIPLINE;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.TOPIC_DOMAIN;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.TOPIC_ORIGIN;
import static eu.dissco.core.digitalspecimenprocessor.util.DigitalObjectUtils.DOI_PREFIX;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaEvent;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaRecord;
import eu.dissco.core.digitalspecimenprocessor.domain.media.UpdatedDigitalMediaTuple;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenRecord;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenWrapper;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.UpdatedDigitalSpecimenTuple;
import eu.dissco.core.digitalspecimenprocessor.property.FdoProperties;
import eu.dissco.core.digitalspecimenprocessor.schema.Agent;
import eu.dissco.core.digitalspecimenprocessor.schema.DigitalMedia;
import eu.dissco.core.digitalspecimenprocessor.schema.DigitalSpecimen.OdsPhysicalSpecimenIDType;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
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
  private static final String URL_PATTERN = "http(s)?://.+";
  private final ObjectMapper mapper;
  private final FdoProperties fdoProperties;

  private static boolean isEqualString(String currentValue, String newValue) {
    return currentValue != null && !currentValue.equals(newValue);
  }

  private static void setLicense(ObjectNode attributes, DigitalMedia media) {
    if (media.getDctermsRights() != null && media.getDctermsRights().matches(URL_PATTERN)) {
      attributes.put(LICENSE_URL.getAttribute(), media.getDctermsRights());
    } else if (media.getDctermsRights() != null) {
      attributes.put(LICENSE_NAME.getAttribute(), media.getDctermsRights());
    }
  }

  private static void setRightsHolder(ObjectNode attributes, DigitalMedia media) {
    var rightsHolderId = collectRightsHolder(media, false);
    var rightsHolderName = collectRightsHolder(media, true);
    if (rightsHolderId != null) {
      attributes.put(RIGHTS_HOLDER_PID.getAttribute(), rightsHolderId);
    }
    if (rightsHolderName != null) {
      attributes.put(RIGHTS_HOLDER.getAttribute(), rightsHolderName);
    }
  }

  private static String collectRightsHolder(DigitalMedia media, boolean name) {
    var rightsHolderStream = media.getOdsHasAgents().stream()
        .filter(agent -> agent.getOdsHasRoles().stream()
            .anyMatch(role -> role.getSchemaRoleName().equals(RIGHTS_OWNER.getName())));
    if (name) {
      return rightsHolderStream.map(Agent::getSchemaName).filter(Objects::nonNull)
          .reduce((a, b) -> a + " | " + b)
          .orElse(null);
    } else {
      return rightsHolderStream.map(Agent::getId).filter(Objects::nonNull)
          .reduce((a, b) -> a + " | " + b).orElse(null);
    }
  }

  public List<JsonNode> buildPostHandleRequest(List<DigitalSpecimenWrapper> digitalSpecimens) {
    return digitalSpecimens.stream().map(this::buildSinglePostHandleRequest).toList();
  }

  public List<JsonNode> buildPostRequestMedia(List<DigitalMediaEvent> digitalMediaList) {
    var requests = new ArrayList<JsonNode>();
    for (var mediaEvent : digitalMediaList) {
      var media = mediaEvent.digitalMediaWrapper().attributes();
      requests.add(mapper.createObjectNode().set("data",
          mapper.createObjectNode()
              .put("type", fdoProperties.getMediaFdoType())
              .set(ATTRIBUTES, generateMediaAttributes(media))));
    }
    return requests;
  }

  private JsonNode generateMediaAttributes(DigitalMedia media){
    var attributes = mapper.createObjectNode()
        .put(REFERENT_NAME.getAttribute(), media.getAcAccessURI())
        .put(MEDIA_HOST.getAttribute(), media.getOdsOrganisationID())
        .put(MEDIA_HOST_NAME.getAttribute(), media.getOdsOrganisationName())
        .put(PRIMARY_MEDIA_ID.getAttribute(), media.getAcAccessURI())
        .put(PRIMARY_MEDIA_ID_TYPE.getAttribute(), "Resolvable")
        .put(PRIMARY_MEDIA_ID_NAME.getAttribute(), "ac:accessURI")
        .put(MEDIA_TYPE.getAttribute(), fdoProperties.getDefaultMediaType())
        .put(MIME_TYPE.getAttribute(), media.getDctermsFormat());
    setLicense(attributes, media);
    setRightsHolder(attributes, media);
    return attributes;
  }

  public List<JsonNode> buildUpdateHandleRequest(
      List<UpdatedDigitalSpecimenTuple> digitalSpecimens) {
    return digitalSpecimens.stream().map(this::buildSingleUpdateHandleRequest).toList();
  }

  public List<JsonNode> buildUpdateHandleRequestMedia(List<UpdatedDigitalMediaTuple> digitalMediaTuples) {
    List<JsonNode> requestBody = new ArrayList<>();
    for (var media : digitalMediaTuples) {
      requestBody.add(buildSingleUpdateRequestMedia(media.digitalMediaEvent(), media.currentDigitalMediaRecord().id()));
    }
    return requestBody;
  }

  public List<JsonNode> buildRollbackUpdateRequest(
      List<DigitalSpecimenRecord> digitalSpecimenRecords) {
    return digitalSpecimenRecords.stream().map(this::buildSingleRollbackUpdateRequest).toList();
  }

  public List<JsonNode> buildRollbackUpdateRequestMedia(List<DigitalMediaRecord> digitalMediaRecords){
    // todo
    return List.of();
  }


  public List<String> buildRollbackCreationRequest(List<DigitalSpecimenRecord> digitalSpecimens) {
    return digitalSpecimens.stream().map(DigitalSpecimenRecord::id).toList();
  }

  private JsonNode buildSinglePostHandleRequest(DigitalSpecimenWrapper specimen) {
    return mapper.createObjectNode()
        .set(DATA, mapper.createObjectNode()
            .put(TYPE, fdoProperties.getSpecimenFdoType())
            .set(ATTRIBUTES, genRequestAttributes(specimen)));
  }

  private JsonNode buildSingleUpdateRequestMedia(DigitalMediaEvent mediaEvent, String id) {
    return mapper.createObjectNode()
        .set("data", mapper.createObjectNode()
            .put("type", fdoProperties.getMediaFdoType())
            .put("id", id.replace(DOI_PREFIX, ""))
            .set(ATTRIBUTES, generateMediaAttributes(mediaEvent.digitalMediaWrapper().attributes())));
  }

  private JsonNode buildSingleUpdateHandleRequest(UpdatedDigitalSpecimenTuple specimenTuple) {
    var request = mapper.createObjectNode();
    var data = mapper.createObjectNode();
    data.put(ID, specimenTuple.currentSpecimen().id().replace(DOI_PREFIX, ""));
    data.put(TYPE, fdoProperties.getSpecimenFdoType());
    var attributes = genRequestAttributes(
        specimenTuple.digitalSpecimenEvent().digitalSpecimenWrapper());
    data.set(ATTRIBUTES, attributes);
    request.set(DATA, data);
    return request;
  }

  private JsonNode buildSingleRollbackUpdateRequest(DigitalSpecimenRecord specimen) {
    return mapper.createObjectNode()
        .set(DATA, mapper.createObjectNode()
            .put(ID, specimen.id().replace(DOI_PREFIX, ""))
            .put(TYPE, fdoProperties.getSpecimenFdoType())
            .set(ATTRIBUTES, genRequestAttributes(specimen.digitalSpecimenWrapper())));

  }

  private JsonNode genRequestAttributes(DigitalSpecimenWrapper specimen) {
    var attributes = mapper.createObjectNode()
        .put(FdoProfileAttributes.NORMALISED_PRIMARY_SPECIMEN_OBJECT_ID.getAttribute(),
            specimen.attributes().getOdsNormalisedPhysicalSpecimenID())
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
    if (specimen.attributes().getOdsTopicOrigin() != null) {
      attributes.put(TOPIC_ORIGIN.getAttribute(),
          specimen.attributes().getOdsTopicOrigin().value());
    }
    if (specimen.attributes().getOdsTopicDomain() != null) {
      attributes.put(TOPIC_DOMAIN.getAttribute(),
          specimen.attributes().getOdsTopicDomain().value());
    }
    if (specimen.attributes().getOdsTopicDiscipline() != null) {
      attributes.put(TOPIC_DISCIPLINE.getAttribute(),
          specimen.attributes().getOdsTopicDiscipline().value());
    }
    if (specimen.attributes().getOdsLivingOrPreserved() != null) {
      attributes.put(LIVING_OR_PRESERVED.getAttribute(),
          specimen.attributes().getOdsLivingOrPreserved().value());
    }
    if (specimen.attributes().getOdsIsMarkedAsType() != null) {
      attributes.put(MARKED_AS_TYPE.getAttribute(), specimen.attributes().getOdsIsMarkedAsType());
    }
    if (specimen.attributes().getOdsSpecimenName() != null) {
      attributes.put(REFERENT_NAME.getAttribute(),
          specimen.attributes().getOdsSpecimenName());
    } else if (specimen.attributes().getOdsOrganisationName() != null) {
      attributes.put(REFERENT_NAME.getAttribute(),
          "Specimen from " + specimen.attributes().getOdsOrganisationName());
    } else {
      attributes.put(REFERENT_NAME.getAttribute(),
          "Specimen from " + specimen.attributes().getOdsOrganisationID());
    }
    if (specimen.attributes().getOdsHasIdentifiers() != null) {
      for (var identifier : specimen.attributes().getOdsHasIdentifiers()) {
        if ("dwc:catalogNumber".equals(identifier.getDctermsTitle())) {
          attributes.put(CATALOG_NUMBER.getAttribute(), identifier.getDctermsIdentifier());
        }
      }
    }
    attributes.set("otherSpecimenIds", buildOtherSpecimenIdArray(specimen));
  }

  private ArrayNode buildOtherSpecimenIdArray(DigitalSpecimenWrapper specimen) {
    var otherSpecimenIds = new HashSet<OtherSpecimenId>();
    otherSpecimenIds.add(new OtherSpecimenId(
        "physical specimen identifier",
        specimen.attributes().getOdsPhysicalSpecimenID(),
        specimen.attributes().getOdsPhysicalSpecimenIDType()
            .equals(OdsPhysicalSpecimenIDType.RESOLVABLE))
    );
    if (specimen.attributes().getOdsHasIdentifiers() != null) {
      for (var id : specimen.attributes().getOdsHasIdentifiers()) {
        otherSpecimenIds.add(new OtherSpecimenId(
            id.getDctermsTitle(),
            id.getDctermsIdentifier(),
            id.getDctermsIdentifier().matches(URL_PATTERN)
        ));
      }
    }
    return mapper.convertValue(otherSpecimenIds, ArrayNode.class);
  }

  public boolean handleNeedsUpdateSpecimen(DigitalSpecimenWrapper currentDigitalSpecimenWrapper,
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

  public boolean handleNeedsUpdateMedia(DigitalMedia currentDigitalMedia,
      DigitalMedia digitalMedia) {
    return false;  // todo
  }

  private record OtherSpecimenId(
      String identifierType, String identifierValue, boolean resolvable
  ) {

  }

}
