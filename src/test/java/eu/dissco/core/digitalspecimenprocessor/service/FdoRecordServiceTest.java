package eu.dissco.core.digitalspecimenprocessor.service;

import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.CREATED;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.HANDLE;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MAPPER;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.ORGANISATION_ID;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.PHYSICAL_SPECIMEN_ID;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.SPECIMEN_NAME;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.TOPIC_DISCIPLINE;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.TYPE;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.generateSpecimenOriginalData;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenAttributes;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenAttributesPlusIdentifier;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenEvent;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenWrapper;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenRecord;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenHandleRequestFullTypeStatus;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenHandleRequestMin;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenUpdateHandleRequest;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mockStatic;

import com.fasterxml.jackson.databind.JsonNode;
import eu.dissco.core.digitalspecimenprocessor.domain.DigitalSpecimenWrapper;
import eu.dissco.core.digitalspecimenprocessor.domain.DigitalSpecimenRecord;
import eu.dissco.core.digitalspecimenprocessor.domain.UpdatedDigitalSpecimenTuple;
import eu.dissco.core.digitalspecimenprocessor.property.FdoProperties;
import eu.dissco.core.digitalspecimenprocessor.schema.DigitalSpecimen.OdsLivingOrPreserved;
import eu.dissco.core.digitalspecimenprocessor.schema.DigitalSpecimen.OdsPhysicalSpecimenIDType;
import eu.dissco.core.digitalspecimenprocessor.schema.DigitalSpecimen.OdsTopicDiscipline;
import eu.dissco.core.digitalspecimenprocessor.utils.TestUtils;
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

  private static final String ORG_NAME = "National Museum of Natural History";
  private static final String REPLACEMENT_ATTRIBUTE = "this is different";
  private final Instant instant = Instant.now(Clock.fixed(CREATED, ZoneOffset.UTC));
  private MockedStatic<Instant> mockedStatic;
  private FdoRecordService builder;
  private MockedStatic<Clock> mockedClock;

  private static eu.dissco.core.digitalspecimenprocessor.schema.DigitalSpecimen givenDigitalSpecimenAttributesMinimal() {
    return new eu.dissco.core.digitalspecimenprocessor.schema.DigitalSpecimen()
        .withOdsOrganisationID(ORGANISATION_ID)
        .withOdsPhysicalSpecimenIDType(OdsPhysicalSpecimenIDType.LOCAL)
        .withOdsNormalisedPhysicalSpecimenID(PHYSICAL_SPECIMEN_ID);
  }

  private static JsonNode givenDigitalSpecimenOriginalAttributesMinimal() {
    var attributeNode = MAPPER.createObjectNode();
    attributeNode.put("ods:organisationId", ORGANISATION_ID);
    return attributeNode;
  }

  private static JsonNode givenDigitalSpecimenAttributesFull(Boolean markedAsType) {
    var attributeNode = MAPPER.createObjectNode();
    attributeNode.put("ods:organisationId", ORGANISATION_ID);
    attributeNode.put("ods:organisationName", ORG_NAME);
    attributeNode.put("ods:specimenName", SPECIMEN_NAME);
    attributeNode.put("ods:topicDiscipline", TOPIC_DISCIPLINE.value());
    attributeNode.put("ods:physicalSpecimenIdType", "cetaf");
    attributeNode.put("ods:livingOrPreserved", "Living");
    if (markedAsType != null) {
      attributeNode.put("ods:markedAsType", markedAsType);
    }
    return attributeNode;
  }

  private static Stream<Arguments> digitalSpecimensNeedToBeChanged() {
    var attributes = givenAttributes(SPECIMEN_NAME, ORGANISATION_ID, true, false);
    return Stream.of(Arguments.of(attributes.withOdsOrganisationID(REPLACEMENT_ATTRIBUTE)),
        Arguments.of(attributes.withOdsOrganisationName(REPLACEMENT_ATTRIBUTE)),
        Arguments.of(attributes.withOdsSpecimenName(REPLACEMENT_ATTRIBUTE)),
        Arguments.of(attributes.withOdsTopicDiscipline(OdsTopicDiscipline.ECOLOGY)),
        Arguments.of(attributes.withOdsLivingOrPreserved(OdsLivingOrPreserved.LIVING)),
        Arguments.of(attributes.withOdsPhysicalSpecimenIDType(OdsPhysicalSpecimenIDType.LOCAL)),
        Arguments.of(attributes.withOdsIsMarkedAsType(false)));
  }

  @BeforeEach
  void setup() {
    builder = new FdoRecordService(MAPPER, new FdoProperties());
    Clock clock = Clock.fixed(CREATED, ZoneOffset.UTC);
    mockedStatic = mockStatic(Instant.class);
    mockedStatic.when(Instant::now).thenReturn(instant);
    mockedClock = mockStatic(Clock.class);
    mockedClock.when(Clock::systemUTC).thenReturn(clock);
  }

  @AfterEach
  void destroy() {
    mockedStatic.close();
    mockedClock.close();
  }

  @Test
  void testGenRequestMinimal() throws Exception {
    // Given
    var specimen = new DigitalSpecimenWrapper(PHYSICAL_SPECIMEN_ID, TYPE,
        givenDigitalSpecimenAttributesMinimal(), givenDigitalSpecimenOriginalAttributesMinimal());
    var expected = new ArrayList<>(
        List.of(givenHandleRequestMin()));

    // When
    var response = builder.buildPostHandleRequest(List.of(specimen));

    // Then
    assertThat(response).isEqualTo(expected);
  }

  @Test
  void testBuildUpdateHandleRequest() throws Exception {
    // Given
    var tupleList = List.of(
        new UpdatedDigitalSpecimenTuple(givenDigitalSpecimenRecord(), givenDigitalSpecimenEvent()));

    // When
    var response = builder.buildUpdateHandleRequest(tupleList);

    // Then
    assertThat(response).isEqualTo(List.of(givenUpdateHandleRequest()));
  }

  @Test
  void testGenRequestMinimalCombined() throws Exception {
    // Given
    var specimen = new DigitalSpecimenWrapper(PHYSICAL_SPECIMEN_ID, TYPE,
        givenDigitalSpecimenAttributesMinimal(), givenDigitalSpecimenOriginalAttributesMinimal());
    specimen.attributes().setOdsPhysicalSpecimenIDType(OdsPhysicalSpecimenIDType.LOCAL);
    var expected = new ArrayList<>(
        List.of(givenHandleRequestMin()));

    // When
    var response = builder.buildPostHandleRequest(List.of(specimen));

    // Then
    assertThat(response).isEqualTo(expected);
  }

  @Test
  void testRollbackUpdate() throws Exception {
    var specimen = new DigitalSpecimenWrapper(PHYSICAL_SPECIMEN_ID, TYPE,
        givenDigitalSpecimenAttributesMinimal(), givenDigitalSpecimenOriginalAttributesMinimal());
    var specimenRecord = new DigitalSpecimenRecord(HANDLE, 1, 1, CREATED, specimen);
    var expected = MAPPER.readTree("""
            {
              "data": {
                "type": "https://hdl.handle.net/21.T11148/894b1e6cad57e921764e",
                "id":"20.5000.1025/V1Z-176-LL4",
                "attributes": {
                  "issuedForAgent": "https://ror.org/0566bfb96",
                  "primarySpecimenObjectId": "https://geocollections.info/specimen/23602",
                  "normalisedPrimarySpecimenObjectId":"https://geocollections.info/specimen/23602",
                  "primarySpecimenObjectIdType":"Local",
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
  @ValueSource(booleans = {true, false})
  void testGenRequestFull(boolean markedAsType) throws Exception {
    // Given
    var specimen = new DigitalSpecimenWrapper(PHYSICAL_SPECIMEN_ID, TYPE,
        givenAttributes(SPECIMEN_NAME, ORGANISATION_ID, markedAsType, false),
        givenDigitalSpecimenAttributesFull(markedAsType));
    var expectedJson = getExpectedJson(markedAsType);
    var expected = new ArrayList<>(List.of(expectedJson));

    // When
    var response = builder.buildPostHandleRequest(List.of(specimen));

    // Then
    assertThat(response).isEqualTo(expected);
  }

  @Test
  void testGenRequestIdentifiers() throws Exception {
    // Given
    var markedAsType = false;
    var specimen = new DigitalSpecimenWrapper(PHYSICAL_SPECIMEN_ID, TYPE,
        givenAttributesPlusIdentifier(SPECIMEN_NAME, ORGANISATION_ID, markedAsType),
        givenDigitalSpecimenAttributesFull(markedAsType));
    var expectedJson = MAPPER.readTree(
          """
          {
            "data": {
              "type": "https://hdl.handle.net/21.T11148/894b1e6cad57e921764e",
              "attributes": {
                "issuedForAgent": "https://ror.org/0566bfb96",
                "primarySpecimenObjectId": "https://geocollections.info/specimen/23602",
                "normalisedPrimarySpecimenObjectId":"https://geocollections.info/specimen/23602",
                "primarySpecimenObjectIdType": "Global",
                "specimenHost": "https://ror.org/0443cwa12",
                "specimenHostName": "National Museum of Natural History",
                "topicDiscipline": "Botany",
                "referentName": "Biota",
                "livingOrPreserved": "Preserved",
                "markedAsType": false,
                "otherSpecimenIds": [{
                  "identifierType": "Specimen label",
                  "identifierValue": "20.5000.1025/V1Z-176-LL4"
                }]
              }
            }
          }
          """
    );
    var expected = new ArrayList<>(List.of(expectedJson));

    // When
    var response = builder.buildPostHandleRequest(List.of(specimen));

    // Then
    assertThat(response).isEqualTo(expected);
  }

  @Test
  void testGenRequestFullTypeIsNull() throws Exception {
    // Given
    var specimen = new DigitalSpecimenWrapper(PHYSICAL_SPECIMEN_ID, TYPE,
        givenAttributes(SPECIMEN_NAME, ORGANISATION_ID, null, false),
        givenDigitalSpecimenAttributesFull(null));
    var expectedJson = getExpectedJson(null);
    var expected = new ArrayList<>(List.of(expectedJson));

    // When
    var response = builder.buildPostHandleRequest(List.of(specimen));

    // Then
    assertThat(response).isEqualTo(expected);
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
  void testHandleNeedsUpdate(
      eu.dissco.core.digitalspecimenprocessor.schema.DigitalSpecimen currentAttributes) {
    var currentDigitalSpecimen = new DigitalSpecimenWrapper(PHYSICAL_SPECIMEN_ID, TYPE,
        currentAttributes,
        givenDigitalSpecimenOriginalAttributesMinimal());
    // Then
    assertThat(
        builder.handleNeedsUpdate(currentDigitalSpecimen, givenDigitalSpecimenWrapper())).isTrue();
  }

  @Test
  void testHandleDoesNotNeedsUpdate() {
    // Given
    var currentDigitalSpecimen = givenAttributes(SPECIMEN_NAME, ORGANISATION_ID,
        null, false).withDwcCollectionID(REPLACEMENT_ATTRIBUTE);

    // Then
    assertThat(builder.handleNeedsUpdate(
        new DigitalSpecimenWrapper(PHYSICAL_SPECIMEN_ID, TYPE, currentDigitalSpecimen,
            generateSpecimenOriginalData()), givenDigitalSpecimenWrapper())).isFalse();
  }

  @Test
  void testPhysicalSpecimenIdsDifferent() {
    // Given
    var currentSpecimen = TestUtils.givenDigitalSpecimenWrapper("ALT ID", SPECIMEN_NAME,
        ORGANISATION_ID, false);

    // When/then
    assertThat(builder.handleNeedsUpdate(currentSpecimen, givenDigitalSpecimenWrapper())).isTrue();
  }

  private JsonNode getExpectedJson(Boolean markedAsType) throws Exception {
    if (markedAsType != null && markedAsType) {
      return givenHandleRequestFullTypeStatus();
    }
    if (markedAsType != null) {
      return MAPPER.readTree("""
          {
            "data": {
              "type": "https://hdl.handle.net/21.T11148/894b1e6cad57e921764e",
              "attributes": {
                "issuedForAgent": "https://ror.org/0566bfb96",
                "primarySpecimenObjectId": "https://geocollections.info/specimen/23602",
                "normalisedPrimarySpecimenObjectId":"https://geocollections.info/specimen/23602",
                "primarySpecimenObjectIdType": "Global",
                "specimenHost": "https://ror.org/0443cwa12",
                "specimenHostName": "National Museum of Natural History",
                "topicDiscipline": "Botany",
                "referentName": "Biota",
                "livingOrPreserved": "Preserved",
                "markedAsType": false
              }
            }
          }
          """);
    }
    return MAPPER.readTree("""
        {
          "data": {
            "type": "https://hdl.handle.net/21.T11148/894b1e6cad57e921764e",
            "attributes": {
              "issuedForAgent": "https://ror.org/0566bfb96",
              "primarySpecimenObjectId": "https://geocollections.info/specimen/23602",
              "normalisedPrimarySpecimenObjectId":"https://geocollections.info/specimen/23602",
              "primarySpecimenObjectIdType": "Global",
              "specimenHost": "https://ror.org/0443cwa12",
              "specimenHostName": "National Museum of Natural History",
              "topicDiscipline": "Botany",
              "referentName": "Biota",
              "livingOrPreserved": "Preserved"
            }
          }
        }""");
  }
}
