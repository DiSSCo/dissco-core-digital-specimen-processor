package eu.dissco.core.digitalspecimenprocessor.service;

import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.LICENSE_ID;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.LICENSE_NAME;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.RIGHTS_HOLDER_ID;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.RIGHTS_HOLDER_NAME;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.CREATED;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.HANDLE;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MAPPER;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MEDIA_URL;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.ORGANISATION_ID;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.ORIGINAL_DATA;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.PHYSICAL_SPECIMEN_ID;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.SPECIMEN_NAME;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.TYPE;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.TYPE_MEDIA;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.generateSpecimenOriginalData;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenAttributes;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenAttributesPlusIdentifier;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalMediaEvent;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenEvent;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenRecord;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenWrapper;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenEmptyMediaProcessResult;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenHandleAttributes;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenHandleMediaRequest;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenHandleMediaRequestAttributes;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenHandleRequestMin;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenUpdateHandleRequest;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mockStatic;

import com.fasterxml.jackson.databind.node.ObjectNode;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaEventWithoutDOI;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaWithoutDOI;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenRecord;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenWrapper;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.UpdatedDigitalSpecimenTuple;
import eu.dissco.core.digitalspecimenprocessor.property.FdoProperties;
import eu.dissco.core.digitalspecimenprocessor.schema.DigitalMedia;
import eu.dissco.core.digitalspecimenprocessor.schema.DigitalSpecimen;
import eu.dissco.core.digitalspecimenprocessor.schema.DigitalSpecimen.OdsLivingOrPreserved;
import eu.dissco.core.digitalspecimenprocessor.schema.DigitalSpecimen.OdsPhysicalSpecimenIDType;
import eu.dissco.core.digitalspecimenprocessor.schema.DigitalSpecimen.OdsTopicDiscipline;
import eu.dissco.core.digitalspecimenprocessor.schema.DigitalSpecimen.OdsTopicDomain;
import eu.dissco.core.digitalspecimenprocessor.schema.DigitalSpecimen.OdsTopicOrigin;
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

  private static final String REPLACEMENT_ATTRIBUTE = "this is different";
  private final Instant instant = Instant.now(Clock.fixed(CREATED, ZoneOffset.UTC));
  private MockedStatic<Instant> mockedStatic;
  private FdoRecordService fdoRecordService;
  private MockedStatic<Clock> mockedClock;

  private static eu.dissco.core.digitalspecimenprocessor.schema.DigitalSpecimen givenDigitalSpecimenAttributesMinimal() {
    return new eu.dissco.core.digitalspecimenprocessor.schema.DigitalSpecimen()
        .withOdsOrganisationID(ORGANISATION_ID)
        .withOdsPhysicalSpecimenIDType(OdsPhysicalSpecimenIDType.LOCAL)
        .withOdsNormalisedPhysicalSpecimenID(PHYSICAL_SPECIMEN_ID)
        .withOdsPhysicalSpecimenID(PHYSICAL_SPECIMEN_ID);
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
    fdoRecordService = new FdoRecordService(MAPPER, new FdoProperties());
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
  void testGenRequestMedia() {
    // Given
    var expected = List.of(givenHandleMediaRequest());

    // When
    var result = fdoRecordService.buildPostRequestMedia(HANDLE, List.of(givenDigitalMediaEvent()));

    // Then
    assertThat(result).isEqualTo(expected);
  }

  @ParameterizedTest
  @MethodSource("genLicenseAndRightsHolder")
  void testGenRequestLicenseAndRightsHolder(String licenseField, String rightsHolderField,
      String fieldValue) {
    // Given
    var media = new DigitalMediaEventWithoutDOI(
        List.of("image-metadata"),
        new DigitalMediaWithoutDOI(
            "StillImage",
            HANDLE,
            new DigitalMedia()
                .withAcAccessURI(MEDIA_URL)
                .withOdsOrganisationID(ORGANISATION_ID)
                .withDctermsLicense(fieldValue)
                .withDctermsRightsHolder(fieldValue),
            MAPPER.createObjectNode()
        )
    );
    var expected = List.of(MAPPER.createObjectNode()
        .set("data", MAPPER.createObjectNode()
            .put("type", TYPE_MEDIA)
            .set("attributes", ((ObjectNode) givenHandleMediaRequestAttributes())
                .put(licenseField, fieldValue)
                .put(rightsHolderField, fieldValue))));

    // When
    var result = fdoRecordService.buildPostRequestMedia(HANDLE, List.of(media));

    // Then
    assertThat(result).isEqualTo(expected);
  }

  static Stream<Arguments> genLicenseAndRightsHolder() {
    return Stream.of(
        Arguments.of(LICENSE_ID.getAttribute(), RIGHTS_HOLDER_ID.getAttribute(),
            "https://spdx.org/licenses/Apache-2.0.html"),
        Arguments.of(LICENSE_NAME.getAttribute(), RIGHTS_HOLDER_NAME.getAttribute(), "Apache 2.0"));
  }

  @Test
  void testGenRequestMinimal() throws Exception {
    // Given
    var specimen = new DigitalSpecimenWrapper(PHYSICAL_SPECIMEN_ID, TYPE,
        givenDigitalSpecimenAttributesMinimal(), ORIGINAL_DATA);
    var expected = new ArrayList<>(
        List.of(givenHandleRequestMin()));

    // When
    var response = fdoRecordService.buildPostHandleRequest(List.of(specimen));

    // Then
    assertThat(response).isEqualTo(expected);
  }

  @Test
  void testBuildUpdateHandleRequest() throws Exception {
    // Given
    var tupleList = List.of(
        new UpdatedDigitalSpecimenTuple(givenDigitalSpecimenRecord(), givenDigitalSpecimenEvent(),
            givenEmptyMediaProcessResult()));

    // When
    var response = fdoRecordService.buildUpdateHandleRequest(tupleList);

    // Then
    assertThat(response).isEqualTo(List.of(givenUpdateHandleRequest(true)));
  }

  @Test
  void testGenRequestMinimalCombined() throws Exception {
    // Given
    var specimen = new DigitalSpecimenWrapper(PHYSICAL_SPECIMEN_ID, TYPE,
        givenDigitalSpecimenAttributesMinimal(), ORIGINAL_DATA);
    specimen.attributes().setOdsPhysicalSpecimenIDType(OdsPhysicalSpecimenIDType.LOCAL);
    var expected = new ArrayList<>(
        List.of(givenHandleRequestMin()));

    // When
    var response = fdoRecordService.buildPostHandleRequest(List.of(specimen));

    // Then
    assertThat(response).isEqualTo(expected);
  }

  @Test
  void testRollbackUpdate() throws Exception {
    var specimen = givenDigitalSpecimenWrapper();
    var specimenRecord = new DigitalSpecimenRecord(HANDLE, 1, 1, CREATED, specimen);
    var expected = givenUpdateHandleRequest(true);

    // When
    var result = fdoRecordService.buildRollbackUpdateRequest(List.of(specimenRecord));

    // Then
    assertThat(result).isEqualTo(List.of(expected));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testGenRequestFull(boolean markedAsType) throws Exception {
    // Given
    var specimen = new DigitalSpecimenWrapper(PHYSICAL_SPECIMEN_ID, TYPE,
        givenAttributes(SPECIMEN_NAME, ORGANISATION_ID, markedAsType, false)
            .withOdsTopicDomain(OdsTopicDomain.EARTH_SYSTEM)
            .withOdsTopicOrigin(OdsTopicOrigin.NATURAL),
        ORIGINAL_DATA);
    var expectedAttributes = (ObjectNode) givenHandleAttributes(markedAsType);
    expectedAttributes.put("topicDomain", OdsTopicDomain.EARTH_SYSTEM.value());
    expectedAttributes.put("topicOrigin", OdsTopicOrigin.NATURAL.value());
    var expected = List.of(MAPPER.createObjectNode()
        .set("data", MAPPER.createObjectNode()
            .put("type", TYPE)
            .set("attributes", expectedAttributes)));

    // When
    var response = fdoRecordService.buildPostHandleRequest(List.of(specimen));

    // Then
    assertThat(response).isEqualTo(expected);
  }

  @Test
  void testGenRequestIdentifiers() throws Exception {
    // Given
    var markedAsType = false;
    var specimen = new DigitalSpecimenWrapper(PHYSICAL_SPECIMEN_ID, TYPE,
        givenAttributesPlusIdentifier(SPECIMEN_NAME, ORGANISATION_ID, markedAsType),
        ORIGINAL_DATA);
    var expectedJson = MAPPER.readTree(
        """
            {
              "data": {
                "type": "https://doi.org/21.T11148/894b1e6cad57e921764e",
                "attributes": {
                  "normalisedPrimarySpecimenObjectId":"https://geocollections.info/specimen/23602",
                  "specimenHost": "https://ror.org/0443cwa12",
                  "specimenHostName": "National Museum of Natural History",
                  "topicDiscipline": "Botany",
                  "referentName": "Biota",
                  "livingOrPreserved": "Preserved",
                  "markedAsType": false,
                  "otherSpecimenIds": [{"identifierType":"Specimen label","identifierValue":"20.5000.1025/V1Z-176-LL4","resolvable":false},{"identifierType":"physical specimen identifier","identifierValue":"https://geocollections.info/specimen/23602","resolvable":false}]}}}]
                }
              }
            }
            """
    );
    var expected = new ArrayList<>(List.of(expectedJson));

    // When
    var response = fdoRecordService.buildPostHandleRequest(List.of(specimen));

    // Then
    assertThat(response).isEqualTo(expected);
  }

  @Test
  void testGenRollbackCreationRequest() {
    // Given
    var digitalSpecimenRecords = List.of(givenDigitalSpecimenRecord());
    var expected = List.of(HANDLE);

    // When
    var response = fdoRecordService.buildRollbackCreationRequest(digitalSpecimenRecords);

    // Then
    assertThat(response).isEqualTo(expected);
  }

  @ParameterizedTest
  @MethodSource("digitalSpecimensNeedToBeChanged")
  void testHandleNeedsUpdate(DigitalSpecimen currentAttributes) {
    var currentDigitalSpecimen = new DigitalSpecimenWrapper(PHYSICAL_SPECIMEN_ID, TYPE,
        currentAttributes,
        ORIGINAL_DATA);
    // Then
    assertThat(
        fdoRecordService.handleNeedsUpdate(currentDigitalSpecimen,
            givenDigitalSpecimenWrapper())).isTrue();
  }

  @Test
  void testHandleDoesNotNeedsUpdate() {
    // Given
    var currentDigitalSpecimen = givenAttributes(SPECIMEN_NAME, ORGANISATION_ID,
        null, false).withDwcCollectionID(REPLACEMENT_ATTRIBUTE);

    // Then
    assertThat(fdoRecordService.handleNeedsUpdate(
        new DigitalSpecimenWrapper(PHYSICAL_SPECIMEN_ID, TYPE, currentDigitalSpecimen,
            generateSpecimenOriginalData()), givenDigitalSpecimenWrapper())).isFalse();
  }

  @Test
  void testPhysicalSpecimenIdsDifferent() {
    // Given
    var currentSpecimen = givenDigitalSpecimenWrapper("ALT ID", SPECIMEN_NAME,
        ORGANISATION_ID, false);

    // When/then
    assertThat(fdoRecordService.handleNeedsUpdate(currentSpecimen,
        givenDigitalSpecimenWrapper())).isTrue();
  }
}
