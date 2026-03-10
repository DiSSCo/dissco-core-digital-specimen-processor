package eu.dissco.core.digitalspecimenprocessor.utils;

import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.CREATED;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.DOI_PREFIX;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.HANDLE;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.HANDLE_PREFIX;

import io.github.dissco.core.annotationlogic.schema.Agent;
import io.github.dissco.core.annotationlogic.schema.Agent.Type;
import io.github.dissco.core.annotationlogic.schema.Annotation;
import io.github.dissco.core.annotationlogic.schema.Annotation.OaMotivation;
import io.github.dissco.core.annotationlogic.schema.Annotation.OdsMergingDecisionStatus;
import io.github.dissco.core.annotationlogic.schema.Annotation.OdsStatus;
import io.github.dissco.core.annotationlogic.schema.AnnotationBody;
import io.github.dissco.core.annotationlogic.schema.AnnotationTarget;
import io.github.dissco.core.annotationlogic.schema.OaHasSelector;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class AnnotationTestUtils {

  public static final String ANNOTATION_FDO_TYPE = "https://doi.org/21.T11148/cf458ca9ee1d44a5608f";
  public static final String NEW_VALUE = "Some new value!";
  public static final String ORCID = "https://orcid.org/0000-0002-XXXX-XXXX";
  public static final String ANNOTATION_ID = "10.2500.1025/888-999-000";
  public static final String ANNOTATION_ID_2 = "10.2500.1025/555-6666-777";
  public static final String ANNOTATION_ID_3 = "10.2500.1025/222-333-444";


  private AnnotationTestUtils() {
    // Utility class
  }

  public static Annotation givenAnnotation(){
    return givenAnnotation(HANDLE_PREFIX + ANNOTATION_ID,  DOI_PREFIX + HANDLE);
  }

  public static Annotation givenAnnotation(String annotationId, String targetId) {
    return givenAnnotation(OaMotivation.ODS_ADDING, true, annotationId, targetId);
  }

  public static Annotation givenAnnotation(OaMotivation motivation, boolean isTermAnnotation,
      String annotationId, String targetId) {
    var target = isTermAnnotation ? givenOaTargetTerm(motivation, targetId)
        : givenAnnotationTargetClass(motivation, null, targetId);
    AnnotationBody body;
    if (motivation.equals(OaMotivation.ODS_DELETING)) {
      body = new AnnotationBody().withOaValue(List.of());
    } else {
      body = isTermAnnotation ? givenOaBodyTerm() : givenOaBodyClass();
    }
    return new Annotation()
        .withId(annotationId)
        .withDctermsIdentifier(annotationId)
        .withType("ods:Annotation")
        .withOdsFdoType(ANNOTATION_FDO_TYPE)
        .withOdsVersion(1)
        .withOdsStatus(OdsStatus.ACTIVE)
        .withOdsMergingDecisionStatus(OdsMergingDecisionStatus.APPROVED)
        .withOaHasBody(body)
        .withOaMotivation(motivation)
        .withOaHasTarget(target)
        .withDctermsCreator(givenAgent(Type.PROV_PERSON))
        .withDctermsCreated(Date.from(CREATED))
        .withDctermsIssued(Date.from(CREATED))
        .withDctermsModified(Date.from(CREATED))
        .withAsGenerator(givenAgent(Type.PROV_SOFTWARE_AGENT));
  }

  private static AnnotationBody givenOaBodyTerm() {
    return new AnnotationBody()
        .withType("oa:TextualBody")
        .withOaValue(new ArrayList<>(List.of(NEW_VALUE)))
        .withDctermsReferences(
            "https://medialib.naturalis.nl/file/id/ZMA.UROCH.P.1555/format/large")
        .withOdsScore(0.99);
  }

  private static AnnotationBody givenOaBodyClass() {
    return new AnnotationBody()
        .withType("oa:TextualBody")
        .withOaValue(new ArrayList<>(List.of("""
            {
              "dwc:habitat": "marsh"
            }
            """)))
        .withDctermsReferences(
            "https://medialib.naturalis.nl/file/id/ZMA.UROCH.P.1555/format/large")
        .withOdsScore(0.99);
  }

  private static AnnotationTarget givenOaTargetTerm(OaMotivation motivation, String targetId) {
    var path = OaMotivation.ODS_ADDING.equals(motivation) ?
        "$['ods:topicOrigin']" :
        "$['ods:topicDiscipline']";
    return givenAnnotationTargetTerm(path, targetId);
  }

  public static AnnotationTarget givenAnnotationTargetTerm(String path, String targetId) {
    return new AnnotationTarget()
        .withId(targetId)
        .withType("ods:DigitalSpecimen")
        .withOdsFdoType(ANNOTATION_FDO_TYPE)
        .withDctermsIdentifier(targetId)
        .withOaHasSelector(
            new OaHasSelector()
                .withAdditionalProperty("@type", "ods:TermSelector")
                .withAdditionalProperty("ods:term", path)
        );
  }

  public static AnnotationTarget givenAnnotationTargetClass(OaMotivation motivation, String path,
      String targetId) {
    if (path == null) {
      path = OaMotivation.ODS_ADDING.equals(motivation) ?
          "$['ods:hasEvents'][*]" :
          "$['ods:hasEntityRelationships'][0]";
    }
    return new AnnotationTarget()
        .withId(targetId)
        .withType("ods:DigitalSpecimen")
        .withOdsFdoType(ANNOTATION_FDO_TYPE)
        .withDctermsIdentifier(targetId)
        .withOaHasSelector(
            new OaHasSelector()
                .withAdditionalProperty("ods:class", path)
                .withAdditionalProperty("@type", "ods:ClassSelector")
        );
  }

  public static Agent givenAgent(Type type) {
    return new Agent()
        .withSchemaName("Some agent")
        .withId(ORCID)
        .withType(type);
  }

}
