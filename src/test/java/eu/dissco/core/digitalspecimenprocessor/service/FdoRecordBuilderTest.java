package eu.dissco.core.digitalspecimenprocessor.service;

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
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import eu.dissco.core.digitalspecimenprocessor.component.FdoRecordBuilder;
import eu.dissco.core.digitalspecimenprocessor.domain.DigitalSpecimen;
import eu.dissco.core.digitalspecimenprocessor.domain.HandleAttribute;
import eu.dissco.core.digitalspecimenprocessor.domain.UpdatedDigitalSpecimenTuple;
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
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerFactory;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class FdoRecordBuilderTest {

  @Mock
  private Random random;
  @Mock
  private HandleRepository repository;
  private MockedStatic<Instant> mockedStatic;
  private static final String HANDLE_PROXY = "https://hdl.handle.net/";
  private static final String TO_FIX = "Needs to be fixed!";
  private static final String UNKNOWN = "Unknown";
  private final DateTimeFormatter dt = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(
      ZoneId.of("UTC"));
  private static final String ORG_NAME = "National Museum of Natural History";
  private static final String TYPE_STATUS = "holotype";
  private static final String DWCA_ID = "ZMA.V.POL.1296.2@CRS";
  private static final String COLL_ID = "NAT.123XYZ.AVES";

  private FdoRecordBuilder builder;
  private final Instant instant = Instant.now(Clock.fixed(CREATED, ZoneOffset.UTC));

  @BeforeEach
  void setup() throws ParserConfigurationException {
    var docFactory = DocumentBuilderFactory.newInstance();
    var transFactory = TransformerFactory.newInstance();
    builder = new FdoRecordBuilder(MAPPER, random, docFactory.newDocumentBuilder(), repository,
        transFactory);
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
    var response = builder.genCreateHandleRequest(List.of(specimen));

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
    var expectedFile = typeStatus.equals("holotype") ? "handlerequests/TestHandleRequestFullTypeStatus.json" : "handlerequests/TestHandleRequestFullNoTypeStatus.json";

    var expected = new ArrayList(List.of(MAPPER.readTree(loadResourceFile(expectedFile))));

    // When
    var response = builder.genCreateHandleRequest(List.of(specimen));

    // Then
    assertThat(response).isEqualTo(expected);
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
  void testCreateNewHandle() throws Exception {
    // Given
    given(random.nextInt(33)).willReturn(21);
    mockedStatic.when(() -> Instant.from(any())).thenReturn(instant);

    // When
    var result = builder.createNewHandle(givenDigitalSpecimen());

    // Then
    then(repository).should().createHandle(eq(GENERATED_HANDLE), eq(CREATED), anyList());
    assertThat(result).isEqualTo(GENERATED_HANDLE);
  }


  @Test
  void testUpdateHandle() {
    // Given

    // When
    builder.updateHandles(List.of(
        new UpdatedDigitalSpecimenTuple(givenUnequalDigitalSpecimenRecord(),
            givenDigitalSpecimenEvent())));

    // Then
    then(repository).should().updateHandleAttributes(eq(HANDLE), eq(CREATED), anyList(), eq(true));
  }

  @Test
  void testRollbackHandleCreation() {
    // Given

    // When
    builder.rollbackHandleCreation(givenDigitalSpecimenRecord());

    // Then
    then(repository).should().rollbackHandleCreation(HANDLE);
  }

  @Test
  void testDeleteVersion() {
    // Given

    // When
    builder.deleteVersion(givenDigitalSpecimenRecord());

    // Then
    then(repository).should().updateHandleAttributes(eq(HANDLE), eq(CREATED), anyList(), eq(false));
  }

/*
  @Test
  void testCheckForPrimarySpecimenObjectIdIsPresent() throws Exception{
    // Given
    var mockRecord = mock(Record.class);
    given(mockRecord.get((Field<Object>) any())).willReturn(GENERATED_HANDLE.getBytes(StandardCharsets.UTF_8));
    var specimen = givenDigitalSpecimenAdditionalAttributes();
    given(repository.searchByPrimarySpecimenObjectId(specimen.physicalSpecimenId().getBytes(
        StandardCharsets.UTF_8))).willReturn(Optional.of(mockRecord));

    Clock clock = Clock.fixed(CREATED, ZoneOffset.UTC);
    try(MockedStatic<Clock> mockedClock = mockStatic(Clock.class)){
      mockedClock.when(Clock::systemUTC).thenReturn(clock);
      builder.createNewHandle(specimen);
    }

    // Then
    then(repository).should().updateHandleAttributes(
        GENERATED_HANDLE, CREATED,givenFdoRecordSpecimenAttributesFull(), true);
  } */

  /*
  @Test
  void testFdoRecordFull() throws Exception{
    // Given
    var specimen = givenDigitalSpecimenAdditionalAttributes();
    given(random.nextInt(33)).willReturn(21);
    List<HandleAttribute> expected = new ArrayList<>();
    expected.addAll(givenFdoRecordGeneratedElements(GENERATED_HANDLE));
    expected.addAll(givenFdoRecordSpecimenAttributesFull());
    given(repository.searchByPrimarySpecimenObjectId(any())).willReturn(Optional.empty());

    // When
    builder.createNewHandle(specimen);

    // Then
    then(repository).should().createHandle(GENERATED_HANDLE, CREATED, expected);
  }*/

  /*
  @Test
  void testFdoRecordMinimal() throws Exception{
    // Given
    var specimen = givenDigitalSpecimen();
    given(random.nextInt(33)).willReturn(21);
    List<HandleAttribute> expected = new ArrayList<>();
    expected.addAll(givenFdoRecordGeneratedElements(GENERATED_HANDLE));
    expected.addAll(givenFdoRecordSpecimenAttributesMinimalAttributes());
    given(repository.searchByPrimarySpecimenObjectId(any())).willReturn(Optional.empty());

    // When
    builder.createNewHandle(specimen);

    // Then
    then(repository).should().createHandle(GENERATED_HANDLE, CREATED, expected);
  }*/


  private List<HandleAttribute> givenFdoRecordGeneratedElements(String handle){
    mockedStatic.when(() -> Instant.from(any())).thenReturn(instant);
    List<HandleAttribute> fdoRecord = new ArrayList<>();
    fdoRecord.add(new HandleAttribute(100, HS_ADMIN.getAttribute(), decodeAdmin()));
    fdoRecord.add(new HandleAttribute(101, LOC.getAttribute(), givenLocString(handle)));
    fdoRecord.add(new HandleAttribute(1, FDO_PROFILE.getAttribute(), (HANDLE_PROXY + "21.T11148/d8de0819e144e4096645").getBytes(StandardCharsets.UTF_8)));
    fdoRecord.add(new HandleAttribute(2, FDO_RECORD_LICENSE.getAttribute(), "https://creativecommons.org/publicdomain/zero/1.0/".getBytes(StandardCharsets.UTF_8)));
    fdoRecord.add(new HandleAttribute(3, DIGITAL_OBJECT_TYPE.getAttribute(), (HANDLE_PROXY + "21.T11148/894b1e6cad57e921764e").getBytes(StandardCharsets.UTF_8)));
    fdoRecord.add(new HandleAttribute(4, DIGITAL_OBJECT_NAME.getAttribute(), "digitalSpecimen".getBytes(StandardCharsets.UTF_8)));
    fdoRecord.add(new HandleAttribute(5, PID.getAttribute(), (HANDLE_PROXY + handle).getBytes(StandardCharsets.UTF_8)));
    fdoRecord.add(new HandleAttribute(6, PID_ISSUER.getAttribute(), TO_FIX.getBytes(StandardCharsets.UTF_8)));
    fdoRecord.add(new HandleAttribute(7, PID_ISSUER_NAME.getAttribute(), TO_FIX.getBytes(StandardCharsets.UTF_8)));
    fdoRecord.add(new HandleAttribute(8, ISSUED_FOR_AGENT.getAttribute(), TO_FIX.getBytes(StandardCharsets.UTF_8)));
    fdoRecord.add(new HandleAttribute(9, ISSUED_FOR_AGENT_NAME.getAttribute(), "DiSSCo".getBytes(StandardCharsets.UTF_8)));
    fdoRecord.add(new HandleAttribute(10, PID_RECORD_ISSUE_DATE.getAttribute(), (dt.format(CREATED)).getBytes(StandardCharsets.UTF_8)));
    fdoRecord.add(new HandleAttribute(11, PID_RECORD_ISSUE_NUMBER.getAttribute(), "1".getBytes(StandardCharsets.UTF_8)));
    fdoRecord.add(new HandleAttribute(12, STRUCTURAL_TYPE.getAttribute(), "digital".getBytes(StandardCharsets.UTF_8)));
    fdoRecord.add(new HandleAttribute(13, PID_STATUS.getAttribute(), "DRAFT".getBytes(StandardCharsets.UTF_8)));
    fdoRecord.add(new HandleAttribute(40, REFERENT_TYPE.getAttribute(), TO_FIX.getBytes(StandardCharsets.UTF_8)));
    fdoRecord.add(new HandleAttribute(41, REFERENT_DOI_NAME.getAttribute(), handle.getBytes(StandardCharsets.UTF_8)));
    fdoRecord.add(new HandleAttribute(43, PRIMARY_REFERENT_TYPE.getAttribute(), "creation".getBytes(StandardCharsets.UTF_8)));
    fdoRecord.add(new HandleAttribute(44, REFERENT.getAttribute(), TO_FIX.getBytes(StandardCharsets.UTF_8)));
    fdoRecord.add(new HandleAttribute(210, OBJECT_TYPE.getAttribute(), "Digital Specimen".getBytes(StandardCharsets.UTF_8)));
    return fdoRecord;
  }

  private List<HandleAttribute> givenFdoRecordSpecimenAttributesFull(){
    List<HandleAttribute> fdoRecord = new ArrayList<>();
    fdoRecord.add(new HandleAttribute(42, REFERENT_NAME.getAttribute(), SPECIMEN_NAME.getBytes(StandardCharsets.UTF_8)));
    fdoRecord.add(new HandleAttribute(200, SPECIMEN_HOST.getAttribute(), ORGANISATION_ID.getBytes(StandardCharsets.UTF_8)));
    fdoRecord.add(new HandleAttribute(201, SPECIMEN_HOST_NAME.getAttribute(), ORG_NAME.getBytes(StandardCharsets.UTF_8)));
    fdoRecord.add(new HandleAttribute(202, PRIMARY_SPECIMEN_OBJECT_ID.getAttribute(), PHYSICAL_SPECIMEN_ID.getBytes(StandardCharsets.UTF_8)));
    fdoRecord.add(new HandleAttribute(203, PRIMARY_SPECIMEN_OBJECT_ID_TYPE.getAttribute(), PHYSICAL_SPECIMEN_TYPE.getBytes(StandardCharsets.UTF_8)));
    fdoRecord.add(new HandleAttribute(204, PRIMARY_SPECIMEN_OBJECT_ID_NAME.getAttribute(), ("Local identifier for collection "+COLL_ID).getBytes(StandardCharsets.UTF_8)));
    fdoRecord.add(new HandleAttribute(206, OTHER_SPECIMEN_IDS.getAttribute(), DWCA_ID.getBytes(StandardCharsets.UTF_8)));
    fdoRecord.add(new HandleAttribute(216, MARKED_AS_TYPE.getAttribute(), "TRUE".getBytes(StandardCharsets.UTF_8)));
        return fdoRecord;
  }

  private List<HandleAttribute> givenFdoRecordSpecimenAttributesMinimalAttributes(){
    List<HandleAttribute> fdoRecord = new ArrayList<>();
    fdoRecord.add(new HandleAttribute(42, REFERENT_NAME.getAttribute(), SPECIMEN_NAME.getBytes(StandardCharsets.UTF_8)));
    fdoRecord.add(new HandleAttribute(200, SPECIMEN_HOST.getAttribute(), ORGANISATION_ID.getBytes(StandardCharsets.UTF_8)));
    fdoRecord.add(new HandleAttribute(201, SPECIMEN_HOST_NAME.getAttribute(), UNKNOWN.getBytes(StandardCharsets.UTF_8)));
    fdoRecord.add(new HandleAttribute(202, PRIMARY_SPECIMEN_OBJECT_ID.getAttribute(), PHYSICAL_SPECIMEN_ID.getBytes(StandardCharsets.UTF_8)));
    fdoRecord.add(new HandleAttribute(203, PRIMARY_SPECIMEN_OBJECT_ID_TYPE.getAttribute(), PHYSICAL_SPECIMEN_TYPE.getBytes(StandardCharsets.UTF_8)));
    fdoRecord.add(new HandleAttribute(216, MARKED_AS_TYPE.getAttribute(), "FALSE".getBytes(StandardCharsets.UTF_8)));
    return fdoRecord;
  }

  private DigitalSpecimen givenDigitalSpecimenAdditionalAttributes(){
    var attributes = (ObjectNode) givenAttributes(SPECIMEN_NAME, ORGANISATION_ID);
    attributes.put("ods:organisationName", ORG_NAME);
    attributes.put("ods:physicalSpecimenCollection", COLL_ID);
    attributes.put("dwc:typeStatus", TYPE_STATUS);
    attributes.remove("dwca:id");
    attributes.put("dwca:id", DWCA_ID);
    return new DigitalSpecimen(PHYSICAL_SPECIMEN_ID, TYPE, attributes, ORIGINAL_DATA);
  }



  private byte[] givenLocString(String handle){
    return ("<locations><location href=\"https://sandbox.dissco.tech/api/v1/specimens/"+handle+"\" "
        + "id=\"0\" weight=\"1\"/><location href=\"https://sandbox.dissco.tech/ds/"+handle+"\" "
        + "id=\"1\" weight=\"0\"/></locations>").getBytes(StandardCharsets.UTF_8);
  }

  private static byte[] decodeAdmin() {
    var admin = "0fff000000153330303a302e4e412f32302e353030302e31303235000000c8";
    byte[] adminByte = new byte[admin.length() / 2];
    for (int i = 0; i < admin.length(); i += 2) {
      adminByte[i / 2] = hexToByte(admin.substring(i, i + 2));
    }
    return adminByte;
  }

  private static byte hexToByte(String hexString) {
    int firstDigit = toDigit(hexString.charAt(0));
    int secondDigit = toDigit(hexString.charAt(1));
    return (byte) ((firstDigit << 4) + secondDigit);
  }

  private static int toDigit(char hexChar) {
    return Character.digit(hexChar, 16);
  }


}
