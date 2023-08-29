package eu.dissco.core.digitalspecimenprocessor.service;

import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.CREATED;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.HANDLE;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MAPPER;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.ORGANISATION_ID;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.ORIGINAL_DATA;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.PHYSICAL_SPECIMEN_ID;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.SPECIMEN_NAME;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.TOPIC_DISCIPLINE;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.TYPE;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenAttributes;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimen;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenRecord;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenHandleRequestFullTypeStatus;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenHandleRequestMin;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.loadResourceFile;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mockStatic;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import eu.dissco.core.digitalspecimenprocessor.domain.DigitalSpecimen;
import eu.dissco.core.digitalspecimenprocessor.domain.DigitalSpecimenRecord;
import eu.dissco.core.digitalspecimenprocessor.exception.PidCreationException;
import eu.dissco.core.digitalspecimenprocessor.service.FdoRecordService;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class FdoRecordServiceTest {

  private MockedStatic<Instant> mockedStatic;
  private static final String ORG_NAME = "National Museum of Natural History";
  private static final String REPLACEMENT_ATTRIBUTE = "this is different";

  private FdoRecordService builder;
  private final Instant instant = Instant.now(Clock.fixed(CREATED, ZoneOffset.UTC));

  @BeforeEach
  void setup() {
    builder = new FdoRecordService(MAPPER);
    mockedStatic = mockStatic(Instant.class);
    mockedStatic.when(Instant::now).thenReturn(instant);
  }

  @AfterEach
  void destroy() {
    mockedStatic.close();
  }

  @Test
  void testGenRequestMinimal() throws Exception {
    // Given
    var specimen = new DigitalSpecimen(PHYSICAL_SPECIMEN_ID, TYPE,
        givenDigitalSpecimenAttributesMinimal(), givenDigitalSpecimenAttributesMinimal());
    var expected = new ArrayList<>(
        List.of(givenHandleRequestMin()));

    // When
    var response = builder.buildPostHandleRequest(List.of(specimen));

    // Then
    assertThat(response).isEqualTo(expected);
  }

  @Test
  void testRollbackUpdate() throws Exception {
    var specimen = new DigitalSpecimen(PHYSICAL_SPECIMEN_ID, TYPE,
        givenDigitalSpecimenAttributesMinimal(), givenDigitalSpecimenAttributesMinimal());
    var specimenRecord = new DigitalSpecimenRecord(HANDLE, 1, 1, CREATED, specimen);
    var expected = MAPPER.readTree("""
            {
              "data": {
                "type": "digitalSpecimen",
                "id":"20.5000.1025/V1Z-176-LL4",
                "attributes": {
                  "fdoProfile": "https://hdl.handle.net/21.T11148/d8de0819e144e4096645",
                  "digitalObjectType": "https://hdl.handle.net/21.T11148/894b1e6cad57e921764e",
                  "issuedForAgent": "https://ror.org/0566bfb96",
                  "primarySpecimenObjectId": "https://geocollections.info/specimen/23602",
                   "primarySpecimenObjectIdType":"local",
                  "specimenHost": "https://ror.org/0443cwa12"
                }
              }
            }
        """);
    // When
    var result = builder.buildRollbackUpdateRequest(List.of(specimenRecord));

    // Then
    assertThat(result).isEqualTo(List.of(expected));
  }

  @ParameterizedTest
  @ValueSource(strings = {"false", "holotype", ""})
  void testGenRequestFull(String typeStatus) throws Exception {
    // Given
    var specimen = new DigitalSpecimen(PHYSICAL_SPECIMEN_ID, TYPE,
        givenDigitalSpecimenAttributesFull(typeStatus),
        givenDigitalSpecimenAttributesFull(typeStatus));
    var expectedJson = getExpectedJson(typeStatus);
    var expected = new ArrayList<>(List.of(expectedJson));

    // When
    var response = builder.buildPostHandleRequest(List.of(specimen));

    // Then
    assertThat(response).isEqualTo(expected);
  }

  private static JsonNode givenDigitalSpecimenAttributesMinimal() {
    var attributeNode = MAPPER.createObjectNode();
    attributeNode.put("ods:organisationId", ORGANISATION_ID);
    return attributeNode;
  }

  private static JsonNode givenDigitalSpecimenAttributesFull(String typeStatus) {
    var attributeNode = MAPPER.createObjectNode();
    attributeNode.put("ods:organisationId", ORGANISATION_ID);
    attributeNode.put("ods:organisationName", ORG_NAME);
    attributeNode.put("ods:specimenName", SPECIMEN_NAME);
    attributeNode.put("ods:topicDiscipline", TOPIC_DISCIPLINE);
    attributeNode.put("ods:physicalSpecimenIdType", "cetaf");
    attributeNode.put("ods:livingOrPreserved", "Living");
    if (!typeStatus.isEmpty()) {
      attributeNode.put("dwc:typeStatus", typeStatus);
    }
    return attributeNode;
  }

  @Test
  void testGenRollbackCreationRequest() {
    // Given
    var digitalSpecimenRecords = List.of(givenDigitalSpecimenRecord());
    var id = digitalSpecimenRecords.get(0).id();
    var dataNode = List.of(MAPPER.createObjectNode().put("id", id));
    var dataArr = MAPPER.valueToTree(dataNode);
    var expected = MAPPER.createObjectNode().set("data", dataArr);

    // When
    var response = builder.buildRollbackCreationRequest(digitalSpecimenRecords);

    // Then
    assertThat(response).isEqualTo(expected);
  }

  @ParameterizedTest
  @MethodSource("digitalSpecimensNeedToBeChanged")
  void testHandleNeedsUpdate(DigitalSpecimen currentDigitalSpecimen) {
    // Then
    assertThat(builder.handleNeedsUpdate(currentDigitalSpecimen, givenDigitalSpecimen())).isTrue();
  }

  @Test
  void testHandleDoesNotNeedsUpdate() {
    // Given
    var currentDigitalSpecimen = makeOneFieldUnique("ods:collectingNumber");

    // Then
    assertThat(builder.handleNeedsUpdate(currentDigitalSpecimen, givenDigitalSpecimen())).isFalse();
  }

  @Test
  void testPhysicalSpecimenIdsDifferent() {
    // Given
    var currentSpecimen = givenDigitalSpecimen("ALT ID", SPECIMEN_NAME, ORGANISATION_ID);

    // When/then
    assertThat(builder.handleNeedsUpdate(currentSpecimen, givenDigitalSpecimen())).isTrue();
  }

  @Test
  void testMissingOrganisationId() {
    // Given
    var attributes = givenAttributes(SPECIMEN_NAME, null);
    var specimen = new DigitalSpecimen(PHYSICAL_SPECIMEN_ID, "Digital Specimen", attributes,
        MAPPER.createObjectNode());

    // When/Then
    assertThrows(PidCreationException.class,
        () -> builder.buildPostHandleRequest(List.of(specimen)));
  }

  private static Stream<Arguments> digitalSpecimensNeedToBeChanged() {
    return Stream.of(Arguments.of(makeOneFieldUnique("ods:organisationId")),
        Arguments.of(makeOneFieldUnique("ods:organisationName")),
        Arguments.of(makeOneFieldUnique("ods:specimenName")),
        Arguments.of(makeOneFieldUnique("ods:topicDiscipline")),
        Arguments.of(makeOneFieldUnique("ods:physicalSpecimenIdType")),
        Arguments.of(makeOneFieldUnique("ods:livingOrPreserved")),
        Arguments.of(makeOneFieldUnique("dwc:typeStatus")));
  }

  private static DigitalSpecimen makeOneFieldUnique(String field) {
    ObjectNode attributes = (ObjectNode) givenAttributes(SPECIMEN_NAME, ORGANISATION_ID);
    attributes.put(field, REPLACEMENT_ATTRIBUTE);
    return new DigitalSpecimen(PHYSICAL_SPECIMEN_ID, TYPE, attributes, ORIGINAL_DATA);
  }

  private JsonNode getExpectedJson(String typeStatus) throws Exception {
    if (typeStatus.equals("holotype")) {
      return givenHandleRequestFullTypeStatus();
    }
    if (typeStatus.equals("false")) {
      return MAPPER.readTree("""
          {
            "data": {
              "type": "digitalSpecimen",
              "attributes": {
                "fdoProfile": "https://hdl.handle.net/21.T11148/d8de0819e144e4096645",
                "digitalObjectType": "https://hdl.handle.net/21.T11148/894b1e6cad57e921764e",
                "issuedForAgent": "https://ror.org/0566bfb96",
                "primarySpecimenObjectId": "https://geocollections.info/specimen/23602",
                "primarySpecimenObjectIdType": "global",
                "specimenHost": "https://ror.org/0443cwa12",
                "specimenHostName": "National Museum of Natural History",
                "referentName": "Biota",
                "topicDiscipline": "Earth Systems",
                "livingOrPreserved": "living",
                "markedAsType": false
              }
            }
          }
          """);
    }
    return MAPPER.readTree("""
        {
          "data": {
            "type": "digitalSpecimen",
            "attributes": {
              "fdoProfile": "https://hdl.handle.net/21.T11148/d8de0819e144e4096645",
              "digitalObjectType": "https://hdl.handle.net/21.T11148/894b1e6cad57e921764e",
              "issuedForAgent": "https://ror.org/0566bfb96",
              "primarySpecimenObjectId": "https://geocollections.info/specimen/23602",
              "primarySpecimenObjectIdType": "global",
              "specimenHost": "https://ror.org/0443cwa12",
              "specimenHostName": "National Museum of Natural History",
              "referentName": "Biota",
              "topicDiscipline": "Earth Systems",
              "livingOrPreserved": "living"
            }
          }
        }""");
  }
}
