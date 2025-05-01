package eu.dissco.core.digitalspecimenprocessor.service;

import static eu.dissco.core.digitalspecimenprocessor.domain.AgentRoleType.PROCESSING_SERVICE;
import static eu.dissco.core.digitalspecimenprocessor.domain.AgentRoleType.SOURCE_SYSTEM;
import static eu.dissco.core.digitalspecimenprocessor.schema.Agent.Type.SCHEMA_SOFTWARE_APPLICATION;
import static eu.dissco.core.digitalspecimenprocessor.schema.Identifier.DctermsType.DOI;
import static eu.dissco.core.digitalspecimenprocessor.schema.Identifier.DctermsType.HANDLE;
import static eu.dissco.core.digitalspecimenprocessor.util.AgentUtils.createMachineAgent;
import static eu.dissco.core.digitalspecimenprocessor.util.DigitalObjectUtils.DOI_PREFIX;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import eu.dissco.core.digitalspecimenprocessor.domain.AutoAcceptedAnnotation;
import eu.dissco.core.digitalspecimenprocessor.domain.FdoType;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaRecord;
import eu.dissco.core.digitalspecimenprocessor.domain.media.UpdatedDigitalMediaRecord;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenRecord;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.UpdatedDigitalSpecimenRecord;
import eu.dissco.core.digitalspecimenprocessor.property.ApplicationProperties;
import eu.dissco.core.digitalspecimenprocessor.schema.AnnotationBody;
import eu.dissco.core.digitalspecimenprocessor.schema.AnnotationProcessingRequest;
import eu.dissco.core.digitalspecimenprocessor.schema.AnnotationProcessingRequest.OaMotivation;
import eu.dissco.core.digitalspecimenprocessor.schema.AnnotationTarget;
import eu.dissco.core.digitalspecimenprocessor.schema.OaHasSelector;
import eu.dissco.core.digitalspecimenprocessor.util.DigitalObjectUtils;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class AnnotationPublisherService {

  public static final String NEW_INFORMATION_MESSAGE = "Received new information from Source System with id: ";
  public static final String OP = "op";
  public static final String FROM = "from";
  private static final String VALUE = "value";
  private static final String TYPE = "@type";
  private final Pattern numericPattern = Pattern.compile("\\d+");

  private final RabbitMqPublisherService publisherService;
  private final ApplicationProperties applicationProperties;
  private final ObjectMapper mapper;

  public void publishAnnotationNewSpecimen(Set<DigitalSpecimenRecord> digitalSpecimenRecords) {
    for (DigitalSpecimenRecord digitalSpecimenRecord : digitalSpecimenRecords) {
      try {
        var annotationProcessingRequest = mapNewSpecimenToAnnotation(digitalSpecimenRecord);
        publisherService.publishAcceptedAnnotation(
            new AutoAcceptedAnnotation(
                createMachineAgent(applicationProperties.getName(),
                    applicationProperties.getPid(), PROCESSING_SERVICE, DOI,
                    SCHEMA_SOFTWARE_APPLICATION),
                annotationProcessingRequest));
      } catch (JsonProcessingException e) {
        log.error("Unable to send auto-accepted annotation for new specimen: {}",
            digitalSpecimenRecord.id(), e);
      }
    }
  }

  public void publishAnnotationNewMedia(Set<DigitalMediaRecord> digitalMediaRecords) {
    for (DigitalMediaRecord digitalMediaRecord : digitalMediaRecords) {
      try {
        var annotationProcessingRequest = mapNewMediaToAnnotation(digitalMediaRecord);
        publisherService.publishAcceptedAnnotation(
            new AutoAcceptedAnnotation(
                createMachineAgent(applicationProperties.getName(), applicationProperties.getPid(),
                    PROCESSING_SERVICE, DOI, SCHEMA_SOFTWARE_APPLICATION),
                annotationProcessingRequest));
      } catch (JsonProcessingException e) {
        log.error("Unable to send auto-accepted annotation for new media: {}",
            digitalMediaRecord.id(), e);
      }
    }
  }

  private AnnotationProcessingRequest mapNewSpecimenToAnnotation(
      DigitalSpecimenRecord digitalSpecimenRecord) throws JsonProcessingException {
    var sourceSystemID = digitalSpecimenRecord.digitalSpecimenWrapper().attributes()
        .getOdsSourceSystemID();
    var sourceSystemName = digitalSpecimenRecord.digitalSpecimenWrapper().attributes()
        .getOdsSourceSystemName();
    return new AnnotationProcessingRequest()
        .withOaMotivation(OaMotivation.ODS_ADDING)
        .withOaMotivatedBy("New information received from Source System with id: "
            + sourceSystemID)
        .withOaHasBody(buildBody(mapper.writeValueAsString(
            DigitalObjectUtils.flattenToDigitalSpecimen(digitalSpecimenRecord)), sourceSystemID))
        .withOaHasTarget(
            buildTarget(digitalSpecimenRecord.id(), FdoType.SPECIMEN, buildNewSelector()))
        .withDctermsCreated(Date.from(Instant.now()))
        .withDctermsCreator(
            createMachineAgent(sourceSystemName, sourceSystemID, SOURCE_SYSTEM, HANDLE,
                SCHEMA_SOFTWARE_APPLICATION));
  }

  private AnnotationProcessingRequest mapNewMediaToAnnotation(
      DigitalMediaRecord digitalMediaRecord) throws JsonProcessingException {
    var sourceSystemID = digitalMediaRecord.attributes()
        .getOdsSourceSystemID();
    var sourceSystemName = digitalMediaRecord.attributes()
        .getOdsSourceSystemName();
    return new AnnotationProcessingRequest()
        .withOaMotivation(OaMotivation.ODS_ADDING)
        .withOaMotivatedBy("New information received from Source System with id: "
            + sourceSystemID)
        .withOaHasBody(
            buildBody(mapper.writeValueAsString(digitalMediaRecord.attributes()), sourceSystemID))
        .withOaHasTarget(buildTarget(digitalMediaRecord.id(), FdoType.MEDIA, buildNewSelector()))
        .withDctermsCreated(Date.from(Instant.now()))
        .withDctermsCreator(
            createMachineAgent(sourceSystemName, sourceSystemID, SOURCE_SYSTEM, HANDLE,
                SCHEMA_SOFTWARE_APPLICATION));
  }

  private AnnotationBody buildBody(String value, String sourceSystemID) {
    return new AnnotationBody()
        .withType("oa:TextualBody")
        .withOaValue(List.of(value))
        .withDctermsReferences(sourceSystemID);
  }

  private AnnotationTarget buildTarget(String id, FdoType type,
      OaHasSelector selector) {
    var targetId = DOI_PREFIX + id;
    return new AnnotationTarget()
        .withId(targetId)
        .withDctermsIdentifier(targetId)
        .withType(type.getPid())
        .withOdsFdoType(type.getPid())
        .withOaHasSelector(selector);
  }

  public void publishAnnotationUpdatedSpecimen(
      Set<UpdatedDigitalSpecimenRecord> updatedDigitalSpecimenRecords) {
    for (var updatedDigitalSpecimenRecord : updatedDigitalSpecimenRecords) {
      try {
        var annotations = convertJsonPatchToAnnotations(
            updatedDigitalSpecimenRecord.digitalSpecimenRecord(),
            updatedDigitalSpecimenRecord.jsonPatch());
        for (var annotationProcessingRequest : annotations) {
          publisherService.publishAcceptedAnnotation(new AutoAcceptedAnnotation(
              createMachineAgent(applicationProperties.getName(), applicationProperties.getPid(),
                  PROCESSING_SERVICE, DOI, SCHEMA_SOFTWARE_APPLICATION),
              annotationProcessingRequest));
        }
      } catch (JsonProcessingException e) {
        log.error("Unable to send auto-accepted annotation for updated specimen: {}",
            updatedDigitalSpecimenRecord.digitalSpecimenRecord().id(), e);
      }
    }
  }

  public void publishAnnotationUpdatedMedia(
      Set<UpdatedDigitalMediaRecord> updatedDigitalMediaRecords) {
    for (var updatedDigitalMediaRecord : updatedDigitalMediaRecords) {
      try {
        var annotations = convertJsonPatchToAnnotations(
            updatedDigitalMediaRecord.digitalMediaRecord(),
            updatedDigitalMediaRecord.jsonPatch());
        for (var annotationProcessingRequest : annotations) {
         publisherService.publishAcceptedAnnotation(new AutoAcceptedAnnotation(
              createMachineAgent(applicationProperties.getName(), applicationProperties.getPid(),
                  PROCESSING_SERVICE, DOI, SCHEMA_SOFTWARE_APPLICATION),
              annotationProcessingRequest));
        }
      } catch (JsonProcessingException e) {
        log.error("Unable to send auto-accepted annotation for updated media: {}",
            updatedDigitalMediaRecord.digitalMediaRecord().id(), e);
      }
    }
  }

  private List<AnnotationProcessingRequest> convertJsonPatchToAnnotations(
      DigitalSpecimenRecord digitalSpecimenRecord, JsonNode jsonNode)
      throws JsonProcessingException {
    var annotations = new ArrayList<AnnotationProcessingRequest>();
    var sourceSystemID = digitalSpecimenRecord.digitalSpecimenWrapper().attributes()
        .getOdsSourceSystemID();
    var sourceSystemName = digitalSpecimenRecord.digitalSpecimenWrapper().attributes()
        .getOdsSourceSystemName();
    var recordNode = (ObjectNode) mapper.convertValue(
        digitalSpecimenRecord.digitalSpecimenWrapper().attributes(), JsonNode.class);
    recordNode.put("id", digitalSpecimenRecord.id());
    for (JsonNode action : jsonNode) {
      var annotationProcessingRequest = new AnnotationProcessingRequest()
          .withOaHasTarget(buildTarget(digitalSpecimenRecord.id(), FdoType.SPECIMEN,
              buildSelector(action.get("path").asText())))
          .withDctermsCreated(Date.from(Instant.now()))
          .withDctermsCreator(
              createMachineAgent(sourceSystemName, sourceSystemID, SOURCE_SYSTEM, HANDLE,
                  SCHEMA_SOFTWARE_APPLICATION));
      processOperation(recordNode, action, annotationProcessingRequest, sourceSystemID,
          sourceSystemName, annotations);
    }
    return annotations;
  }


  private List<AnnotationProcessingRequest> convertJsonPatchToAnnotations(
      DigitalMediaRecord digitalMediaRecord, JsonNode jsonNode)
      throws JsonProcessingException {
    var annotations = new ArrayList<AnnotationProcessingRequest>();
    var sourceSystemID = digitalMediaRecord.attributes()
        .getOdsSourceSystemID();
    var sourceSystemName = digitalMediaRecord.attributes()
        .getOdsSourceSystemName();
    var recordNode = mapper.convertValue(digitalMediaRecord.attributes(), JsonNode.class);
    for (JsonNode action : jsonNode) {
      var annotationProcessingRequest = new AnnotationProcessingRequest()
          .withOaHasTarget(buildTarget(digitalMediaRecord.id(), FdoType.MEDIA,
              buildSelector(action.get("path").asText())))
          .withDctermsCreated(Date.from(Instant.now()))
          .withDctermsCreator(
              createMachineAgent(sourceSystemName, sourceSystemID, SOURCE_SYSTEM, HANDLE,
                  SCHEMA_SOFTWARE_APPLICATION));
      processOperation(recordNode, action, annotationProcessingRequest, sourceSystemID,
          sourceSystemName, annotations);
    }
    return annotations;
  }

  private void processOperation(JsonNode recordNode, JsonNode action,
      AnnotationProcessingRequest annotationProcessingRequest, String sourceSystemID,
      String sourceSystemName,
      ArrayList<AnnotationProcessingRequest> annotations)
      throws JsonProcessingException {
    if (action.get(OP).asText().equals("replace")) {
      annotations.add(addReplaceOperation(action, annotationProcessingRequest, sourceSystemID));
    } else if (action.get(OP).asText().equals("add")) {
      annotations.add(addAddOperation(action, annotationProcessingRequest, sourceSystemID));
    } else if (action.get(OP).asText().equals("remove")) {
      annotations.add(addRemoveOperation(annotationProcessingRequest, sourceSystemID));
    } else if (action.get(OP).asText().equals("copy")) {
      var annotation = addCopyOperation(recordNode, action, annotationProcessingRequest,
          sourceSystemID);
      if (annotation != null) {
        annotations.add(annotation);
      }
    } else if (action.get(OP).asText().equals("move")) {
      annotations.addAll(
          addMoveOperation(recordNode, action, annotationProcessingRequest,
              sourceSystemID,
              sourceSystemName, recordNode.get("id").asText()));
    }
  }

  private List<AnnotationProcessingRequest> addMoveOperation(
      JsonNode recordNode, JsonNode action,
      AnnotationProcessingRequest annotationProcessingRequest, String sourceSystemID,
      String sourceSystemName, String id)
      throws JsonProcessingException {
    var valueNode = recordNode.at(action.get(FROM).asText());
    annotationProcessingRequest.setOaMotivation(OaMotivation.ODS_ADDING);
    annotationProcessingRequest.setOaMotivatedBy(NEW_INFORMATION_MESSAGE + sourceSystemID);
    annotationProcessingRequest.setOaHasBody(
        buildBody(extractValueString(valueNode), sourceSystemID));
    var additionalDeleteAnnotation = new AnnotationProcessingRequest()
        .withOaHasTarget(buildTarget(id, FdoType.SPECIMEN,
            buildSelector(action.get(FROM).asText())))
        .withDctermsCreated(Date.from(Instant.now()))
        .withDctermsCreator(
            createMachineAgent(sourceSystemName, sourceSystemID, SOURCE_SYSTEM, HANDLE,
                SCHEMA_SOFTWARE_APPLICATION))
        .withOaMotivation(OaMotivation.ODS_DELETING)
        .withOaMotivatedBy(
            "Received delete information from Source System with id: " + sourceSystemID);
    return List.of(additionalDeleteAnnotation, annotationProcessingRequest);
  }

  private AnnotationProcessingRequest addCopyOperation(JsonNode recordNode,
      JsonNode action, AnnotationProcessingRequest annotationProcessingRequest,
      String sourceSystemID)
      throws JsonProcessingException {
    var valueNode = recordNode.at(action.get(FROM).asText());
    if (!valueNode.isMissingNode()) {
      annotationProcessingRequest.setOaMotivation(OaMotivation.ODS_ADDING);
      annotationProcessingRequest.setOaMotivatedBy(NEW_INFORMATION_MESSAGE + sourceSystemID);
      annotationProcessingRequest.setOaHasBody(
          buildBody(extractValueString(valueNode), sourceSystemID));
      return annotationProcessingRequest;
    } else {
      log.warn("Invalid copy operation in json patch: {} Ignoring this annotation", action);
      return null;
    }
  }

  private AnnotationProcessingRequest addRemoveOperation(
      AnnotationProcessingRequest annotationProcessingRequest,
      String sourceSystemID) {
    annotationProcessingRequest.setOaMotivation(OaMotivation.ODS_DELETING);
    annotationProcessingRequest.setOaMotivatedBy(
        "Received delete information from Source System with id: " + sourceSystemID);
    return annotationProcessingRequest;
  }

  private AnnotationProcessingRequest addAddOperation(JsonNode action,
      AnnotationProcessingRequest annotationProcessingRequest, String sourceSystemID)
      throws JsonProcessingException {
    annotationProcessingRequest.setOaMotivation(OaMotivation.ODS_ADDING);
    annotationProcessingRequest.setOaMotivatedBy(NEW_INFORMATION_MESSAGE + sourceSystemID);
    annotationProcessingRequest.setOaHasBody(
        buildBody(extractValueString(action.get(VALUE)), sourceSystemID));
    return annotationProcessingRequest;
  }

  private AnnotationProcessingRequest addReplaceOperation(JsonNode action,
      AnnotationProcessingRequest annotationProcessingRequest, String sourceSystemID)
      throws JsonProcessingException {
    annotationProcessingRequest.setOaMotivation(OaMotivation.OA_EDITING);
    annotationProcessingRequest.setOaMotivatedBy(
        "Received update information from Source System with id: " + sourceSystemID);
    annotationProcessingRequest.setOaHasBody(
        buildBody(extractValueString(action.get(VALUE)), sourceSystemID));
    return annotationProcessingRequest;
  }

  private String extractValueString(JsonNode action) throws JsonProcessingException {
    if (action.isTextual()) {
      return action.textValue();
    } else {
      return mapper.writeValueAsString(action);
    }
  }

  private OaHasSelector buildSelector(String action) {
    var path = convertJsonPointToJsonPath(action);
    if (action.endsWith("/-")) {
      return new OaHasSelector()
          .withAdditionalProperty(TYPE, "ods:ClassSelector")
          .withAdditionalProperty("ods:class", path);
    } else {
      return new OaHasSelector()
          .withAdditionalProperty(TYPE, "ods:TermSelector")
          .withAdditionalProperty("ods:term", path);
    }
  }

  private static OaHasSelector buildNewSelector() {
    return new OaHasSelector()
        .withAdditionalProperty(TYPE, "ods:ClassSelector")
        .withAdditionalProperty("ods:class", "$");
  }

  public String convertJsonPointToJsonPath(String jsonPointer) {
    String[] parts = jsonPointer.split("/");
    StringBuilder jsonPath = new StringBuilder("$");

    // Start from 1 to ignore the first root element
    for (int i = 1; i < parts.length; i++) {
      String part = parts[i];
      if (isNumeric(part)) {
        jsonPath.append("[").append(part).append("]");
      } else if (!part.equals("-")) {
        jsonPath.append("['").append(part).append("']");
      }
    }
    return jsonPath.toString();
  }

  private boolean isNumeric(String str) {
    return numericPattern.matcher(str).matches();
  }
}
