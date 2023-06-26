package eu.dissco.core.digitalspecimenprocessor.component;

import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.DIGITAL_OBJECT_NAME;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.DIGITAL_OBJECT_TYPE;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.FDO_PROFILE;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.FDO_RECORD_LICENSE;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.HS_ADMIN;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.ISSUED_FOR_AGENT;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.ISSUED_FOR_AGENT_NAME;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.LOC;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.MARKED_AS_TYPE;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.OBJECT_TYPE;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.OTHER_SPECIMEN_IDS;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.PID;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.PID_ISSUER;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.PID_ISSUER_NAME;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.PID_RECORD_ISSUE_DATE;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.PID_RECORD_ISSUE_NUMBER;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.PID_STATUS;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.PRIMARY_REFERENT_TYPE;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.PRIMARY_SPECIMEN_OBJECT_ID;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.PRIMARY_SPECIMEN_OBJECT_ID_NAME;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.PRIMARY_SPECIMEN_OBJECT_ID_TYPE;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.REFERENT;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.REFERENT_DOI_NAME;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.REFERENT_NAME;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.REFERENT_TYPE;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.SPECIMEN_HOST;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.SPECIMEN_HOST_NAME;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.STRUCTURAL_TYPE;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.CREATED;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.GENERATED_HANDLE;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.HANDLE;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MAPPER;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.ORGANISATION_ID;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.ORIGINAL_DATA;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.PHYSICAL_SPECIMEN_ID;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.PHYSICAL_SPECIMEN_TYPE;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.SPECIMEN_NAME;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.TOPIC_DISCIPLINE;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.TYPE;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenAttributes;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimen;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenEvent;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenRecord;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenUnequalDigitalSpecimenRecord;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.loadResourceFile;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.mockStatic;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import eu.dissco.core.digitalspecimenprocessor.domain.DigitalSpecimen;
import eu.dissco.core.digitalspecimenprocessor.domain.HandleAttribute;
import eu.dissco.core.digitalspecimenprocessor.domain.UpdatedDigitalSpecimenTuple;
import eu.dissco.core.digitalspecimenprocessor.exception.PidCreationException;
import eu.dissco.core.digitalspecimenprocessor.repository.HandleRepository;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class FdoRecordBuilderTest {
  private MockedStatic<Instant> mockedStatic;
  private static final String ORG_NAME = "National Museum of Natural History";
  private static final String REPLACEMENT_ATTRIBUTE = "this is different";

  private FdoRecordBuilder builder;
  private final Instant instant = Instant.now(Clock.fixed(CREATED, ZoneOffset.UTC));

  @BeforeEach
  void setup() {
    builder = new FdoRecordBuilder(MAPPER);
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
    var specimen = new DigitalSpecimen(
            PHYSICAL_SPECIMEN_ID,
            TYPE,
            givenDigitalSpecimenAttributesMinimal(),
            givenDigitalSpecimenAttributesMinimal()
    );
    var expected = new ArrayList(List.of(MAPPER.readTree(loadResourceFile("handlerequests/TestHandleRequestMin.json"))));

    // When
    var response = builder.buildPostHandleRequest(List.of(specimen));

    // Then
    assertThat(response).isEqualTo(expected);
  }

  @ParameterizedTest
  @ValueSource(strings = {"false", "holotype", ""})
  void testGenRequestFull(String typeStatus) throws Exception {
    // Given
    var specimen = new DigitalSpecimen(
            PHYSICAL_SPECIMEN_ID,
            TYPE,
            givenDigitalSpecimenAttributesFull(typeStatus),
            givenDigitalSpecimenAttributesFull(typeStatus)
    );
    var expectedFile =getExpectedFile(typeStatus);

    var expected = new ArrayList<>(List.of(MAPPER.readTree(loadResourceFile(expectedFile))));

    // When
    var response = builder.buildPostHandleRequest(List.of(specimen));

    // Then
    assertThat(response).isEqualTo(expected);
  }

  private String getExpectedFile(String typeStatus){
    if (typeStatus.equals("holotype")) return "handlerequests/TestHandleRequestFullTypeStatus.json";
    if (typeStatus.equals("false")) return "handlerequests/TestHandleRequestFullTypeStatusFalse.json";
    return "handlerequests/TestHandleRequestFullNoTypeStatus.json";
  }

  private static JsonNode givenDigitalSpecimenAttributesMinimal(){
    var attributeNode = MAPPER.createObjectNode();
    attributeNode.put("ods:organisationId", ORGANISATION_ID);
    return attributeNode;
  }

  private static JsonNode givenDigitalSpecimenAttributesFull(String typeStatus){
    var attributeNode = MAPPER.createObjectNode();
    attributeNode.put("ods:organisationId", ORGANISATION_ID);
    attributeNode.put("ods:organisationName", ORG_NAME);
    attributeNode.put("ods:specimenName", SPECIMEN_NAME);
    attributeNode.put("ods:topicDiscipline", TOPIC_DISCIPLINE);
    attributeNode.put("ods:physicalSpecimenIdType", "cetaf");
    attributeNode.put("ods:livingOrPreserved", "Living");
    if(!typeStatus.isEmpty()){
      attributeNode.put("dwc:typeStatus", typeStatus);
    }
    return attributeNode;
  }

  @Test
  void testGenRollbackCreationRequest(){
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
  void testHandleNeedsUpdate(DigitalSpecimen currentDigitalSpecimen){
    // Then
    assertThat(builder.handleNeedsUpdate(currentDigitalSpecimen, givenDigitalSpecimen())).isTrue();
  }

  @Test
  void testHandleDoesNotNeedsUpdate(){
    // Given
    var currentDigitalSpecimen = makeOneFieldUnique("ods:collectingNumber");

    // Then
    assertThat(builder.handleNeedsUpdate(currentDigitalSpecimen, givenDigitalSpecimen())).isFalse();
  }

  @Test
  void testPhysicalSpecimenIdsDifferent(){
    // Given
    var currentSpecimen = givenDigitalSpecimen("ALT ID", SPECIMEN_NAME, ORGANISATION_ID);

    // When/then
    assertThat(builder.handleNeedsUpdate(currentSpecimen, givenDigitalSpecimen())).isTrue();
  }

  @Test
  void testMissingOrganisationId(){
    // Given
    var attributes = givenAttributes(SPECIMEN_NAME, null);
    var specimen = new DigitalSpecimen(PHYSICAL_SPECIMEN_ID, "Digital Specimen", attributes, MAPPER.createObjectNode());

    // When/Then
    assertThrows(PidCreationException.class, () -> builder.buildPostHandleRequest(List.of(specimen)));
  }

  private static Stream<Arguments> digitalSpecimensNeedToBeChanged(){
    return Stream.of(
        Arguments.of(makeOneFieldUnique("ods:organisationId")),
        Arguments.of(makeOneFieldUnique("ods:organisationName")),
        Arguments.of(makeOneFieldUnique("ods:specimenName")),
        Arguments.of(makeOneFieldUnique("ods:topicDiscipline")),
        Arguments.of(makeOneFieldUnique("ods:physicalSpecimenIdType")),
        Arguments.of(makeOneFieldUnique("ods:livingOrPreserved")),
        Arguments.of(makeOneFieldUnique("dwc:typeStatus"))
    );
  }

  private static DigitalSpecimen makeOneFieldUnique(String field){
    ObjectNode attributes = (ObjectNode) givenAttributes(SPECIMEN_NAME, ORGANISATION_ID);
    attributes.put(field, REPLACEMENT_ATTRIBUTE);
    return new DigitalSpecimen(PHYSICAL_SPECIMEN_ID, TYPE, attributes, ORIGINAL_DATA);
  }

}
