package eu.dissco.core.digitalspecimenprocessor.service;

import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.APP_HANDLE;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.APP_NAME;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.CREATED;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.DOI_PREFIX;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.HANDLE;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MAPPER;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.SOURCE_SYSTEM_ID;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.SOURCE_SYSTEM_NAME;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.TYPE;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenAutoAcceptedAnnotation;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenRecord;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenEmptyMediaProcessResult;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenNewAcceptedAnnotation;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import eu.dissco.core.digitalspecimenprocessor.domain.AgentRoleType;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.UpdatedDigitalSpecimenRecord;
import eu.dissco.core.digitalspecimenprocessor.property.ApplicationProperties;
import eu.dissco.core.digitalspecimenprocessor.schema.Agent.Type;
import eu.dissco.core.digitalspecimenprocessor.schema.AnnotationBody;
import eu.dissco.core.digitalspecimenprocessor.schema.AnnotationProcessingRequest;
import eu.dissco.core.digitalspecimenprocessor.schema.AnnotationProcessingRequest.OaMotivation;
import eu.dissco.core.digitalspecimenprocessor.schema.AnnotationTarget;
import eu.dissco.core.digitalspecimenprocessor.schema.Identifier.DctermsType;
import eu.dissco.core.digitalspecimenprocessor.schema.OaHasSelector;
import eu.dissco.core.digitalspecimenprocessor.util.AgentUtils;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class AnnotationPublisherServiceTest {

  @Mock
  private KafkaPublisherService kafkaPublisherService;
  @Mock
  private ApplicationProperties applicationProperties;

  private MockedStatic<Instant> mockedInstant;
  private MockedStatic<Clock> mockedClock;
  private AnnotationPublisherService service;

  public static Stream<Arguments> provideUpdateAnnotations() throws JsonProcessingException {
    var classObject = MAPPER.readValue(
        """
            [{
              "op": "add",
              "path": "/ods:hasEntityRelationship/-",
              "value": {
                "@type": "ods:EntityRelationship",
                "dwc:relationshipOfResource": "hasColID",
                "dwc:relatedResourceID": "https://www.catalogueoflife.org/data/taxon/38KTF",
                "dwc:relationshipEstablishedDate": "2024-08-09T13:01:21.688Z",
                "ods:RelationshipAccordingToAgent": {
                  "@id": "https://hdl.handle.net/TEST/123-123-123",
                  "@type": "as:Application",
                  "schema:name": "dissco-nusearch-service",
                  "ods:hasIdentifier": []
                },
                "dwc:relationshipAccordingTo": "dissco-nusearch-service"
              }
            }]
            """, JsonNode.class
    );
    return Stream.of(
        Arguments.of(MAPPER.readTree(
                """
                      [{
                        "op": "replace",
                        "path": "/ods:hasIdentification/0/ods:hasTaxonIdentification/0/dwc:scientificName",
                        "value": "Echinorhynchus haeruca Rudolphi, 1809"
                      }]
                    """),
            List.of(givenAcceptedAnnotation(OaMotivation.OA_EDITING,
                new OaHasSelector().withAdditionalProperty("@type", "ods:TermSelector")
                    .withAdditionalProperty("ods:term",
                        "$['ods:hasIdentification'][0]['ods:hasTaxonIdentification'][0]['dwc:scientificName']"),
                new AnnotationBody().withOaValue(List.of("Echinorhynchus haeruca Rudolphi, 1809"))
                    .withType("oa:TextualBody").withDctermsReferences(SOURCE_SYSTEM_ID)))
        ),
        Arguments.of(classObject,
            List.of(givenAcceptedAnnotation(OaMotivation.ODS_ADDING,
                new OaHasSelector().withAdditionalProperty("@type", "ods:ClassSelector")
                    .withAdditionalProperty("ods:class",
                        "$['ods:hasEntityRelationship']"),
                new AnnotationBody().withOaValue(
                        List.of(MAPPER.writeValueAsString(classObject.get(0).get("value"))))
                    .withType("oa:TextualBody").withDctermsReferences(SOURCE_SYSTEM_ID)
            ))),
        Arguments.of(MAPPER.readTree(
                """
                      [{
                        "op": "remove",
                        "path": "/ods:hasEntityRelationship/1/dwc:relationshipEstablishedDate"
                      }]
                    """),
            List.of(givenAcceptedAnnotation(OaMotivation.ODS_DELETING,
                new OaHasSelector().withAdditionalProperty("@type", "ods:TermSelector")
                    .withAdditionalProperty("ods:term",
                        "$['ods:hasEntityRelationship'][1]['dwc:relationshipEstablishedDate']"),
                null))
        ),
        Arguments.of(
            MAPPER.readTree(
                """
                      [{
                        "op": "copy",
                        "path": "/ods:sourceSystemName",
                        "from": "/ods:sourceSystemID"
                      }]
                    """),
            List.of(givenAcceptedAnnotation(OaMotivation.ODS_ADDING,
                new OaHasSelector().withAdditionalProperty("@type", "ods:TermSelector")
                    .withAdditionalProperty("ods:term",
                        "$['ods:sourceSystemName']"),
                new AnnotationBody().withOaValue(List.of(SOURCE_SYSTEM_ID))
                    .withType("oa:TextualBody").withDctermsReferences(SOURCE_SYSTEM_ID)))
        ),
        Arguments.of(
            MAPPER.readTree(
                """
                      [{
                        "op": "move",
                        "path": "/ods:sourceSystemName",
                        "from": "/ods:sourceSystemID"
                      }]
                    """),
            List.of(givenAcceptedAnnotation(OaMotivation.ODS_ADDING,
                    new OaHasSelector().withAdditionalProperty("@type", "ods:TermSelector")
                        .withAdditionalProperty("ods:term",
                            "$['ods:sourceSystemName']"),
                    new AnnotationBody().withOaValue(List.of(SOURCE_SYSTEM_ID))
                        .withType("oa:TextualBody").withDctermsReferences(SOURCE_SYSTEM_ID)),
                givenAcceptedAnnotation(OaMotivation.ODS_DELETING, new OaHasSelector()
                    .withAdditionalProperty("@type", "ods:TermSelector")
                    .withAdditionalProperty("ods:term", "$['ods:sourceSystemID']"), null))
        ));
  }

  private static AnnotationProcessingRequest givenAcceptedAnnotation(OaMotivation motivation,
      OaHasSelector selector, AnnotationBody body) {
    var annotation = new AnnotationProcessingRequest()
        .withOaMotivation(motivation)
        .withOaHasBody(body)
        .withOaHasTarget(new AnnotationTarget()
            .withOdsFdoType("https://doi.org/21.T11148/894b1e6cad57e921764e")
            .withType(TYPE)
            .withId(DOI_PREFIX + HANDLE)
            .withDctermsIdentifier(DOI_PREFIX + HANDLE)
            .withOaHasSelector(selector))
        .withDctermsCreated(Date.from(CREATED))
        .withDctermsCreator(AgentUtils.createMachineAgent(SOURCE_SYSTEM_NAME, SOURCE_SYSTEM_ID,
            AgentRoleType.SOURCE_SYSTEM, DctermsType.HANDLE, Type.SCHEMA_SOFTWARE_APPLICATION));
    if (motivation == OaMotivation.OA_EDITING) {
      annotation.withOaMotivatedBy("Received update information from Source System with id: "
          + SOURCE_SYSTEM_ID);
    }
    if (motivation == OaMotivation.ODS_ADDING) {
      annotation.withOaMotivatedBy("Received new information from Source System with id: "
          + SOURCE_SYSTEM_ID);
    }
    if (motivation == OaMotivation.ODS_DELETING) {
      annotation.withOaMotivatedBy("Received delete information from Source System with id: "
          + SOURCE_SYSTEM_ID);
    }
    return annotation;
  }

  @BeforeEach
  void setup() {
    service = new AnnotationPublisherService(kafkaPublisherService, applicationProperties, MAPPER);
    Clock clock = Clock.fixed(CREATED, ZoneOffset.UTC);
    Instant instant = Instant.now(clock);
    mockedInstant = mockStatic(Instant.class);
    mockedInstant.when(Instant::now).thenReturn(instant);
    mockedInstant.when(() -> Instant.from(any())).thenReturn(instant);
    mockedInstant.when(() -> Instant.parse(any())).thenReturn(instant);
    mockedClock = mockStatic(Clock.class);
    mockedClock.when(Clock::systemUTC).thenReturn(clock);
  }

  @AfterEach
  void destroy() {
    mockedInstant.close();
    mockedClock.close();
  }

  @Test
  void testPublishAnnotationNewSpecimen() throws JsonProcessingException {
    // Given
    given(applicationProperties.getPid()).willReturn(APP_HANDLE);
    given(applicationProperties.getName()).willReturn(APP_NAME);

    // When
    service.publishAnnotationNewSpecimen(Set.of(givenDigitalSpecimenRecord()));

    // Then
    then(kafkaPublisherService).should()
        .publishAcceptedAnnotation(givenAutoAcceptedAnnotation(givenNewAcceptedAnnotation()));
  }

  @ParameterizedTest
  @MethodSource("provideUpdateAnnotations")
  void testPublishAnnotationUpdatedSpecimen(JsonNode jsonPatch,
      List<AnnotationProcessingRequest> expectedAnnotations) throws JsonProcessingException {

    // Given
    given(applicationProperties.getPid()).willReturn(APP_HANDLE);
    given(applicationProperties.getName()).willReturn(APP_NAME);

    // When
    service.publishAnnotationUpdatedSpecimen(
        Set.of(new UpdatedDigitalSpecimenRecord(givenDigitalSpecimenRecord(),
            List.of(), null, jsonPatch, List.of(), givenEmptyMediaProcessResult())));

    // Then
    then(kafkaPublisherService).should()
        .publishAcceptedAnnotation(givenAutoAcceptedAnnotation(expectedAnnotations));

  }

  @Test
  void testPublishAnnotationUpdatedSpecimenMultiple() throws JsonProcessingException {
    // Given
    given(applicationProperties.getPid()).willReturn(APP_HANDLE);
    given(applicationProperties.getName()).willReturn(APP_NAME);

    // When
    service.publishAnnotationUpdatedSpecimen(
        Set.of(new UpdatedDigitalSpecimenRecord(givenDigitalSpecimenRecord(),
            List.of(), null, givenLargeJsonPatch(), List.of(), givenEmptyMediaProcessResult())));

    // Then
    then(kafkaPublisherService).should(times(1))
        .publishAcceptedAnnotation(any());
  }

  @Test
  void testInvalidCopy() throws JsonProcessingException {
    // Given
    var jsonPatch = MAPPER.readTree(
        """
              [{
                "op": "copy",
                "path": "/ods:sourceSystemName",
                "from": "/ods:someUnknownTerm"
              }]
            """);

    // When
    service.publishAnnotationUpdatedSpecimen(
        Set.of(new UpdatedDigitalSpecimenRecord(givenDigitalSpecimenRecord(),
            List.of(), null, jsonPatch, List.of(), givenEmptyMediaProcessResult())));

    // Then
    then(kafkaPublisherService).shouldHaveNoInteractions();
  }

  private JsonNode givenLargeJsonPatch() throws JsonProcessingException {
    return MAPPER.readTree(
        """
            [
                  {
                    "op": "replace",
                    "path": "/ods:type",
                    "value": "https://doi.org/21.T11148/894b1e6cad57e921764e"
                  },
                  {
                    "op": "replace",
                    "path": "/dcterms:created",
                    "value": "2024-08-09T13:01:53.423Z"
                  },
                  {
                    "op": "remove",
                    "path": "/dcterms:modified"
                  },
                  {
                    "op": "replace",
                    "path": "/ods:version",
                    "value": 2
                  },
                  {
                    "op": "add",
                    "path": "/ods:hasIdentification/0/dwc:verbatimIdentification",
                    "value": "Echinorhynchus haernca"
                  },
                  {
                    "op": "add",
                    "path": "/ods:hasIdentification/0/ods:hasTaxonIdentification/0/dwc:class",
                    "value": "Palaeacanthocephala Meyer, 1931"
                  },
                  {
                    "op": "add",
                    "path": "/ods:hasIdentification/0/ods:hasTaxonIdentification/0/dwc:phylum",
                    "value": "Acanthocephala Rudolphi, 1802"
                  },
                  {
                    "op": "add",
                    "path": "/ods:hasIdentification/0/ods:hasTaxonIdentification/0/dwc:subfamily",
                    "value": "Echinorhynchinae Cobbold, 1876"
                  },
                  {
                    "op": "add",
                    "path": "/ods:hasIdentification/0/ods:hasTaxonIdentification/0/dwc:taxonID",
                    "value": "38KTF"
                  },
                  {
                    "op": "add",
                    "path": "/ods:hasIdentification/0/ods:hasTaxonIdentification/0/ods:scientificNameHtmlLabel",
                    "value": "<i>Echinorhynchus haeruca</i> Rudolphi, 1809"
                  },
                  {
                    "op": "add",
                    "path": "/ods:hasIdentification/0/ods:hasTaxonIdentification/0/dwc:namePublishedInYear",
                    "value": "1809"
                  },
                  {
                    "op": "add",
                    "path": "/ods:hasIdentification/0/ods:hasTaxonIdentification/0/dwc:taxonomicStatus",
                    "value": "SYNONYM"
                  },
                  {
                    "op": "add",
                    "path": "/ods:hasIdentification/0/ods:hasTaxonIdentification/0/dwc:scientificNameAuthorship",
                    "value": "Rudolphi, 1809"
                  },
                  {
                    "op": "add",
                    "path": "/ods:hasIdentification/0/ods:hasTaxonIdentification/0/dwc:acceptedNameUsageID",
                    "value": "64CX5"
                  },
                  {
                    "op": "add",
                    "path": "/ods:hasIdentification/0/ods:hasTaxonIdentification/0/dwc:genericName",
                    "value": "Echinorhynchus"
                  },
                  {
                    "op": "add",
                    "path": "/ods:hasIdentification/0/ods:hasTaxonIdentification/0/dwc:family",
                    "value": "Echinorhynchidae Cobbold, 1876"
                  },
                  {
                    "op": "add",
                    "path": "/ods:hasIdentification/0/ods:hasTaxonIdentification/0/dwc:acceptedNameUsage",
                    "value": "Acanthocephalus ranae (Schrank, 1788)"
                  },
                  {
                    "op": "add",
                    "path": "/ods:hasIdentification/0/ods:hasTaxonIdentification/0/dwc:order",
                    "value": "Echinorhynchida Southwell & Macfie, 1925"
                  },
                  {
                    "op": "replace",
                    "path": "/ods:hasIdentification/0/ods:hasTaxonIdentification/0/dwc:genus",
                    "value": "Acanthocephalus Koelreuter, 1771"
                  },
                  {
                    "op": "replace",
                    "path": "/ods:hasIdentification/0/ods:hasTaxonIdentification/0/dwc:taxonRank",
                    "value": "SPECIES"
                  },
                  {
                    "op": "replace",
                    "path": "/ods:hasIdentification/0/ods:hasTaxonIdentification/0/dwc:scientificName",
                    "value": "Echinorhynchus haeruca Rudolphi, 1809"
                  },
                  {
                    "op": "replace",
                    "path": "/ods:hasIdentification/0/ods:hasTaxonIdentification/0/dwc:specificEpithet",
                    "value": "haeruca"
                  },
                  {
                    "op": "add",
                    "path": "/ods:hasEntityRelationship/0/dwc:relationshipEstablishedDate",
                    "value": "2024-08-09T13:01:11.408Z"
                  },
                  {
                    "op": "remove",
                    "path": "/ods:hasEntityRelationship/1/dwc:relationshipEstablishedDate",
                    "value": "2024-08-09T13:01:11.408Z"
                  },
                  {
                    "op": "add",
                    "path": "/ods:hasEntityRelationship/2/dwc:relationshipEstablishedDate",
                    "value": "2024-08-09T13:01:11.408Z"
                  },
                  {
                    "op": "replace",
                    "path": "/ods:hasEntityRelationship/2/dwc:relatedResourceID",
                    "value": "https://doi.org/21.T11148/894b1e6cad57e921764e"
                  },
                  {
                    "op": "add",
                    "path": "/ods:hasEntityRelationship/3/dwc:relationshipEstablishedDate",
                    "value": "2024-08-09T13:01:11.408Z"
                  },
                  {
                    "op": "add",
                    "path": "/ods:hasEntityRelationship/-",
                    "value": {
                      "@type": "ods:EntityRelationship",
                      "dwc:relationshipOfResource": "hasColID",
                      "dwc:relatedResourceID": "https://www.catalogueoflife.org/data/taxon/38KTF",
                      "dwc:relationshipEstablishedDate": "2024-08-09T13:01:21.688Z",
                      "ods:RelationshipAccordingToAgent": {
                        "@id": "https://hdl.handle.net/TEST/123-123-123",
                        "@type": "as:Application",
                        "schema:name": "dissco-nusearch-service",
                        "ods:hasIdentifier": []
                      },
                      "dwc:relationshipAccordingTo": "dissco-nusearch-service"
                    }
                  },
                  {
                    "op": "replace",
                    "path": "/ods:specimenName",
                    "value": "Echinorhynchus haeruca Rudolphi, 1809"
                  }
                ]
            """
    );
  }

}
