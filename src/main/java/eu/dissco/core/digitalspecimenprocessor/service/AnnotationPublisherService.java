package eu.dissco.core.digitalspecimenprocessor.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dissco.core.digitalspecimenprocessor.domain.AutoAcceptedAnnotation;
import eu.dissco.core.digitalspecimenprocessor.domain.DigitalSpecimenRecord;
import eu.dissco.core.digitalspecimenprocessor.domain.UpdatedDigitalSpecimenRecord;
import eu.dissco.core.digitalspecimenprocessor.property.ApplicationProperties;
import eu.dissco.core.digitalspecimenprocessor.schema.Agent;
import eu.dissco.core.digitalspecimenprocessor.schema.Agent.Type;
import eu.dissco.core.digitalspecimenprocessor.schema.AnnotationBody;
import eu.dissco.core.digitalspecimenprocessor.schema.AnnotationProcessingRequest;
import eu.dissco.core.digitalspecimenprocessor.schema.AnnotationProcessingRequest.OaMotivation;
import eu.dissco.core.digitalspecimenprocessor.schema.AnnotationTarget;
import eu.dissco.core.digitalspecimenprocessor.schema.OaHasSelector;
import eu.dissco.core.digitalspecimenprocessor.util.DigitalSpecimenUtils;
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

  private static final String VALUE = "value";
  private static final String TYPE = "@type";
  private final Pattern numericPattern = Pattern.compile("\\d+");

  private final KafkaPublisherService kafkaPublisherService;
  private final ApplicationProperties applicationProperties;
  private final ObjectMapper mapper;

  private static OaHasSelector buildNewSpecimenSelector() {
    return new OaHasSelector()
        .withAdditionalProperty(TYPE, "ods:ClassSelector")
        .withAdditionalProperty("ods:class", "$");
  }

  public void publishAnnotationNewSpecimen(Set<DigitalSpecimenRecord> digitalSpecimenRecords) {
    for (DigitalSpecimenRecord digitalSpecimenRecord : digitalSpecimenRecords) {
      try {
        var annotationProcessingRequest = mapNewSpecimenToAnnotation(digitalSpecimenRecord);
        kafkaPublisherService.publishAcceptedAnnotation(
            new AutoAcceptedAnnotation(
                new Agent().withId(applicationProperties.getPid())
                    .withSchemaName(applicationProperties.getName())
                    .withType(Type.AS_APPLICATION),
                annotationProcessingRequest));
      } catch (JsonProcessingException e) {
        log.error("Unable to send auto-accepted annotation for new specimen: {}",
            digitalSpecimenRecord.id(), e);
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
            DigitalSpecimenUtils.flattenToDigitalSpecimen(digitalSpecimenRecord)), sourceSystemID))
        .withOaHasTarget(buildTarget(digitalSpecimenRecord, buildNewSpecimenSelector()))
        .withDctermsCreated(Date.from(Instant.now()))
        .withDctermsCreator(
            new Agent().withType(Type.AS_APPLICATION).withId(sourceSystemID)
                .withSchemaName(sourceSystemName));
  }

  private AnnotationBody buildBody(String value, String sourceSystemID) {
    return new AnnotationBody()
        .withType("oa:TextualBody")
        .withOaValue(List.of(value))
        .withDctermsReferences(sourceSystemID);
  }

  private AnnotationTarget buildTarget(DigitalSpecimenRecord digitalSpecimenRecord,
      OaHasSelector selector) {
    return new AnnotationTarget()
        .withId(digitalSpecimenRecord.id())
        .withOdsID(digitalSpecimenRecord.id())
        .withType(digitalSpecimenRecord.digitalSpecimenWrapper().type())
        .withOdsType("ods:DigitalSpecimen")
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
          kafkaPublisherService.publishAcceptedAnnotation(new AutoAcceptedAnnotation(
              new Agent().withId(applicationProperties.getPid())
                  .withSchemaName(applicationProperties.getName())
                  .withType(Type.AS_APPLICATION),
              annotationProcessingRequest));
        }
      } catch (JsonProcessingException e) {
        log.error("Unable to send auto-accepted annotation for updated specimen: {}",
            updatedDigitalSpecimenRecord.digitalSpecimenRecord().id(), e);
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
    for (JsonNode action : jsonNode) {
      var annotationProcessingRequest = new AnnotationProcessingRequest()
          .withOaHasTarget(buildTarget(digitalSpecimenRecord, buildSpecimenSelector(action)))
          .withDctermsCreated(Date.from(Instant.now()))
          .withDctermsCreator(
              new Agent().withType(Type.AS_APPLICATION).withId(sourceSystemID)
                  .withSchemaName(sourceSystemName));
      if (action.get("op").asText().equals("replace")) {
        annotationProcessingRequest.setOaMotivation(OaMotivation.OA_EDITING);
        annotationProcessingRequest.setOaMotivatedBy(
            "Received update information from Source System with id: " + sourceSystemID);
        annotationProcessingRequest.setOaHasBody(
            buildBody(extractValueString(action), sourceSystemID));
      } else if (action.get("op").asText().equals("add")) {
        annotationProcessingRequest.setOaMotivation(OaMotivation.ODS_ADDING);
        annotationProcessingRequest.setOaMotivatedBy(
            "Received new information from Source System with id: " + sourceSystemID);
        annotationProcessingRequest.setOaHasBody(
            buildBody(extractValueString(action), sourceSystemID));
      } else if (action.get("op").asText().equals("remove")) {
        annotationProcessingRequest.setOaMotivation(OaMotivation.ODS_DELETING);
        annotationProcessingRequest.setOaMotivatedBy(
            "Received delete information from Source System with id: " + sourceSystemID);
      }
      annotations.add(annotationProcessingRequest);
    }
    return annotations;
  }

  private String extractValueString(JsonNode action) throws JsonProcessingException {
    if (action.get(VALUE).isTextual()) {
      return action.get(VALUE).textValue();
    } else {
      return mapper.writeValueAsString(action.get(VALUE));
    }
  }

  private OaHasSelector buildSpecimenSelector(JsonNode action) {
    var pointer = action.get("path").asText();
    var path = convertJsonPointToJsonPath(pointer);
    if (action.get("path").asText().endsWith("/-")) {
      return new OaHasSelector()
          .withAdditionalProperty(TYPE, "ods:ClassSelector")
          .withAdditionalProperty("ods:class", path);
    } else {
      return new OaHasSelector()
          .withAdditionalProperty(TYPE, "ods:FieldSelector")
          .withAdditionalProperty("ods:field", path);
    }
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
        jsonPath.append(".").append(part);
      }
    }
    return jsonPath.toString();
  }

  private boolean isNumeric(String str) {
    return numericPattern.pattern().matches(str);
  }
}
