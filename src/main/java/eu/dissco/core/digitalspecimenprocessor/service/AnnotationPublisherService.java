package eu.dissco.core.digitalspecimenprocessor.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dissco.core.digitalspecimenprocessor.domain.AutoAcceptedAnnotation;
import eu.dissco.core.digitalspecimenprocessor.domain.DigitalSpecimenRecord;
import eu.dissco.core.digitalspecimenprocessor.property.ApplicationProperties;
import eu.dissco.core.digitalspecimenprocessor.schema.Agent;
import eu.dissco.core.digitalspecimenprocessor.schema.Agent.Type;
import eu.dissco.core.digitalspecimenprocessor.schema.AnnotationBody;
import eu.dissco.core.digitalspecimenprocessor.schema.AnnotationProcessingRequest;
import eu.dissco.core.digitalspecimenprocessor.schema.AnnotationProcessingRequest.OaMotivation;
import eu.dissco.core.digitalspecimenprocessor.schema.AnnotationTarget;
import eu.dissco.core.digitalspecimenprocessor.schema.OaHasSelector;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@AllArgsConstructor
public class AnnotationPublisherService {

  private final KafkaPublisherService kafkaPublisherService;
  private final ApplicationProperties applicationProperties;
  private final ObjectMapper mapper;

  private static OaHasSelector buildSelector() {
    return new OaHasSelector()
        .withAdditionalProperty("@type", "ods:ClassSelector")
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
    return new AnnotationProcessingRequest()
        .withOaMotivation(OaMotivation.ODS_ADDING)
        .withOaMotivatedBy("New information received from Source System with id: "
            + digitalSpecimenRecord.digitalSpecimenWrapper().attributes().getOdsSourceSystemID())
        .withOaHasBody(buildBody(digitalSpecimenRecord))
        .withOaHasTarget(buildTarget(digitalSpecimenRecord))
        .withDctermsCreated(Date.from(Instant.now()))
        .withDctermsCreator(
            new Agent().withType(Type.AS_APPLICATION).withId(applicationProperties.getPid())
                .withSchemaName(applicationProperties.getName()));
  }

  private AnnotationBody buildBody(DigitalSpecimenRecord digitalSpecimenRecord)
      throws JsonProcessingException {
    return new AnnotationBody()
        .withType("oa:TextualBody")
        .withOaValue(List.of(mapper.writeValueAsString(
            digitalSpecimenRecord.digitalSpecimenWrapper().attributes())))
        .withDctermsReferences(
            digitalSpecimenRecord.digitalSpecimenWrapper().attributes().getOdsSourceSystemID());
  }

  private AnnotationTarget buildTarget(DigitalSpecimenRecord digitalSpecimenRecord) {
    return new AnnotationTarget()
        .withId(digitalSpecimenRecord.id())
        .withOdsID(digitalSpecimenRecord.id())
        .withType(digitalSpecimenRecord.digitalSpecimenWrapper().type())
        .withOdsType("ods:DigitalSpecimen")
        .withOaHasSelector(buildSelector());
  }
}
