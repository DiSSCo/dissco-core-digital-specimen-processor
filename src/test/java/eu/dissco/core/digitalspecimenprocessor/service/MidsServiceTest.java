package eu.dissco.core.digitalspecimenprocessor.service;

import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MAPPER;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.ORGANISATION_ID;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.PHYSICAL_SPECIMEN_ID;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.PHYSICAL_SPECIMEN_TYPE;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.SOURCE_SYSTEM_ID;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.SPECIMEN_NAME;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.TYPE;
import static org.assertj.core.api.Assertions.assertThat;

import eu.dissco.core.digitalspecimenprocessor.domain.DigitalSpecimenWrapper;
import eu.dissco.core.digitalspecimenprocessor.schema.DigitalSpecimen;
import eu.dissco.core.digitalspecimenprocessor.schema.DigitalSpecimen.OdsTopicDiscipline;
import eu.dissco.core.digitalspecimenprocessor.schema.DwcGeologicalContext;
import eu.dissco.core.digitalspecimenprocessor.schema.GeoReference;
import eu.dissco.core.digitalspecimenprocessor.schema.Identifications;
import eu.dissco.core.digitalspecimenprocessor.schema.Location;
import eu.dissco.core.digitalspecimenprocessor.schema.Occurrences;
import eu.dissco.core.digitalspecimenprocessor.schema.TaxonIdentification;
import eu.dissco.core.digitalspecimenprocessor.utils.TestUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class MidsServiceTest {

  private MidsService midsService;

  private static Stream<Arguments> provideDigitalSpecimen() {
    return Stream.of(
        Arguments.of(TestUtils.givenDigitalSpecimenWrapper(), 0),
        Arguments.of(
            new DigitalSpecimenWrapper(PHYSICAL_SPECIMEN_ID, TYPE, givenPreparationValue(null),
                MAPPER.createObjectNode()), 0),
        Arguments.of(
            new DigitalSpecimenWrapper(PHYSICAL_SPECIMEN_ID, TYPE, givenPreparationValue("      "),
                MAPPER.createObjectNode()), 0),
        Arguments.of(
            new DigitalSpecimenWrapper(PHYSICAL_SPECIMEN_ID, TYPE, givenPreparationValue("null"),
                MAPPER.createObjectNode()), 0),
        Arguments.of(
            new DigitalSpecimenWrapper(PHYSICAL_SPECIMEN_ID, TYPE, givenMissingTopic(),
                MAPPER.createObjectNode()), 0),
        Arguments.of(
            new DigitalSpecimenWrapper(PHYSICAL_SPECIMEN_ID, TYPE,
                givenPreparationValue("in alcohol"),
                MAPPER.createObjectNode()), 1),
        Arguments.of(new DigitalSpecimenWrapper(PHYSICAL_SPECIMEN_ID, TYPE, givenMissingLongitude(),
            MAPPER.createObjectNode()), 1),
        Arguments.of(new DigitalSpecimenWrapper(PHYSICAL_SPECIMEN_ID, TYPE, givenMissingCountry(),
            MAPPER.createObjectNode()), 1),
        Arguments.of(new DigitalSpecimenWrapper(PHYSICAL_SPECIMEN_ID, TYPE, givenInvalidType(),
            MAPPER.createObjectNode()), 1),
        Arguments.of(
            new DigitalSpecimenWrapper(PHYSICAL_SPECIMEN_ID, TYPE,
                givenBotanySpecimen(Boolean.FALSE),
                MAPPER.createObjectNode()), 1),
        Arguments.of(
            new DigitalSpecimenWrapper(PHYSICAL_SPECIMEN_ID, TYPE,
                givenBotanySpecimen(null),
                MAPPER.createObjectNode()), 1),
        Arguments.of(
            new DigitalSpecimenWrapper(PHYSICAL_SPECIMEN_ID, TYPE,
                givenBotanySpecimenMissingFieldNumber(),
                MAPPER.createObjectNode()), 1),
        Arguments.of(
            new DigitalSpecimenWrapper(PHYSICAL_SPECIMEN_ID, TYPE, givenMissingLocation(),
                MAPPER.createObjectNode()), 1),
        Arguments.of(
            new DigitalSpecimenWrapper(PHYSICAL_SPECIMEN_ID, TYPE,
                givenBotanyNoOccurrenceSpecimen(List.of()),
                MAPPER.createObjectNode()), 1),
        Arguments.of(
            new DigitalSpecimenWrapper(PHYSICAL_SPECIMEN_ID, TYPE,
                givenBotanyNoOccurrenceSpecimen(null),
                MAPPER.createObjectNode()), 1),
        Arguments.of(
            new DigitalSpecimenWrapper(PHYSICAL_SPECIMEN_ID, TYPE,
                givenBotanyNoOccurrenceSpecimen(new ArrayList<>() {
                  {
                    add(null);
                  } }),
                MAPPER.createObjectNode()), 1),
        Arguments.of(
            new DigitalSpecimenWrapper(PHYSICAL_SPECIMEN_ID, TYPE,
                givenMidsThreeGenericSpecimen(new Location().withGeoReference(new GeoReference())),
                MAPPER.createObjectNode()), 1),
        Arguments.of(
            new DigitalSpecimenWrapper(PHYSICAL_SPECIMEN_ID, TYPE,
                givenMidsThreeGenericSpecimen(new Location().withGeoReference(null)),
                MAPPER.createObjectNode()), 1),
        Arguments.of(
            new DigitalSpecimenWrapper(PHYSICAL_SPECIMEN_ID, TYPE,
                givenBotanySpecimen(Boolean.TRUE),
                MAPPER.createObjectNode()), 2),
        Arguments.of(
            new DigitalSpecimenWrapper(PHYSICAL_SPECIMEN_ID, TYPE,
                givenBotanySpecimenAlternative(),
                MAPPER.createObjectNode()), 2),
        Arguments.of(
            new DigitalSpecimenWrapper(PHYSICAL_SPECIMEN_ID, TYPE, givenFullPaleoSpecimen(),
                MAPPER.createObjectNode()), 2),
        Arguments.of(
            new DigitalSpecimenWrapper(PHYSICAL_SPECIMEN_ID, TYPE,
                givenFullBioSpecimenMissingTax(true, false),
                MAPPER.createObjectNode()), 2),
        Arguments.of(
            new DigitalSpecimenWrapper(PHYSICAL_SPECIMEN_ID, TYPE,
                givenFullBioSpecimenMissingTax(false, true),
                MAPPER.createObjectNode()), 2),
        Arguments.of(
            new DigitalSpecimenWrapper(PHYSICAL_SPECIMEN_ID, TYPE,
                givenMidsThreeGenericSpecimen(givenMissingUncertaintyLocation()),
                MAPPER.createObjectNode()), 2),
        Arguments.of(
            new DigitalSpecimenWrapper(PHYSICAL_SPECIMEN_ID, TYPE,
                givenMidsThreeGenericSpecimen(givenMissingPercisionLocation()),
                MAPPER.createObjectNode()), 2),
        Arguments.of(
            new DigitalSpecimenWrapper(PHYSICAL_SPECIMEN_ID, TYPE,
                givenMidsThreeGenericSpecimen(givenLocationId()),
                MAPPER.createObjectNode()), 3),
        Arguments.of(
            new DigitalSpecimenWrapper(PHYSICAL_SPECIMEN_ID, TYPE, givenMidsThreeBioSpecimen(),
                MAPPER.createObjectNode()), 3),
        Arguments.of(
            new DigitalSpecimenWrapper(PHYSICAL_SPECIMEN_ID, TYPE,
                givenMidsThreeGenericSpecimen(givenFootprintLocation()),
                MAPPER.createObjectNode()), 3),
        Arguments.of(
            new DigitalSpecimenWrapper(PHYSICAL_SPECIMEN_ID, TYPE,
                givenMidsThreeGenericSpecimen(givenVerbatimCoordinateLocation()),
                MAPPER.createObjectNode()), 3),
        Arguments.of(
            new DigitalSpecimenWrapper(PHYSICAL_SPECIMEN_ID, TYPE,
                givenMidsThreeGenericSpecimen(givenVerbatimLatLongLocation()),
                MAPPER.createObjectNode()), 3)
    );
  }

  private static DigitalSpecimen givenFullBioSpecimenMissingTax(boolean missingName,
      boolean missingIdentificationId) {
    return baseDigitalSpecimen().withDwcPreparations("single specimen")
        .withDwcRecordedById("ORCID")
        .withOdsTopicDiscipline(OdsTopicDiscipline.BOTANY)
        .withDwcIdentification(List.of(new Identifications().withDwcIdentificationID(missingIdentificationId ? null : "ID1")
            .withTaxonIdentifications(
                List.of(new TaxonIdentification().withDwcScientificNameId(missingName ? null : "ID2")))))
        .withOdsHasMedia(true)
        .withOdsMarkedAsType(false)
        .withDwcOccurrence(List.of(new Occurrences()
            .withDwcVerbatimEventDate("A verbatim date as is on the label")
            .withDwcFieldNumber("1202")
            .withDctermsLocation(new Location()
                .withDwcCountry("Estonia")
                .withDwcCountryCode("EE")
                .withGeoReference(new GeoReference()
                    .withDwcDecimalLatitude(59.465625)
                    .withDwcDecimalLongitude(25.059035)
                    .withDwcGeodeticDatum("WGS84")
                    .withDwcCoordinateUncertaintyInMeters(10.0)
                    .withDwcCoordinatePrecision(0.0001))
                .withDwcGeologicalContext(new DwcGeologicalContext().withDwcGroup("Group")))));
  }

  private static DigitalSpecimen givenBotanySpecimenMissingFieldNumber() {
    return baseDigitalSpecimen().withDwcPreparations("in alcohol")
        .withOdsTopicDiscipline(OdsTopicDiscipline.BOTANY)
        .withDwcRecordedBy("sam Leeflang")
        .withOdsMarkedAsType(false)
        .withDwcOccurrence(List.of(
            new Occurrences()
                .withDwcYear(2023)
                .withDctermsLocation(
                    new Location().withDwcCountry("Estonia").withDwcCountryCode("EE")
                        .withGeoReference(new GeoReference()
                            .withDwcDecimalLatitude(59.465625)
                            .withDwcDecimalLongitude(25.059035)))))
        .withOdsHasMedia(true);
  }

  private static DigitalSpecimen givenBotanyNoOccurrenceSpecimen(List<Occurrences> occurrences) {
    return baseDigitalSpecimen().withDwcPreparations("in alcohol")
        .withOdsTopicDiscipline(OdsTopicDiscipline.BOTANY)
        .withDwcRecordedBy("sam Leeflang")
        .withOdsMarkedAsType(true)
        .withDwcOccurrence(occurrences)
        .withOdsHasMedia(true);
  }

  private static DigitalSpecimen givenBotanySpecimenAlternative() {
    return baseDigitalSpecimen().withDwcPreparations("in alcohol")
        .withOdsTopicDiscipline(OdsTopicDiscipline.BOTANY)
        .withDwcRecordedBy("sam Leeflang")
        .withOdsMarkedAsType(false)
        .withDwcOccurrence(List.of(
            new Occurrences()
                .withDwcYear(2023)
                .withDwcRecordNumber("A record number")
                .withDctermsLocation(
                    new Location().withDwcCountry("Estonia").withDwcCountryCode("EE")
                        .withGeoReference(new GeoReference()
                            .withDwcDecimalLatitude(59.465625)
                            .withDwcDecimalLongitude(25.059035)))))
        .withOdsHasMedia(true);
  }

  private static DigitalSpecimen givenMissingLocation() {
    return baseDigitalSpecimen().withDwcPreparations("single specimen")
        .withOdsMarkedAsType(true)
        .withDwcRecordedById("ORCID")
        .withOdsTopicDiscipline(OdsTopicDiscipline.PALAEONTOLOGY)
        .withDwcIdentification(List.of(new Identifications().withDwcIdentificationID("ID1")
            .withTaxonIdentifications(
                List.of(new TaxonIdentification().withDwcScientificNameId("ID2")))))
        .withDwcOccurrence(List.of(new Occurrences()));
  }

  private static DigitalSpecimen givenMidsThreeBioSpecimen() {
    return baseDigitalSpecimen().withDwcPreparations("single specimen")
        .withOdsMarkedAsType(true)
        .withDwcRecordedById("ORCID")
        .withOdsTopicDiscipline(OdsTopicDiscipline.BOTANY)
        .withDwcIdentification(List.of(new Identifications().withDwcIdentificationID("ID1")
            .withTaxonIdentifications(
                List.of(new TaxonIdentification().withDwcScientificNameId("ID2")))))
        .withOdsHasMedia(true)
        .withOdsMarkedAsType(false)
        .withDwcOccurrence(List.of(new Occurrences()
            .withDwcVerbatimEventDate("A verbatim date as is on the label")
            .withDwcFieldNumber("1202")
            .withDctermsLocation(new Location()
                .withDwcCountry("Estonia")
                .withDwcCountryCode("EE")
                .withGeoReference(new GeoReference()
                    .withDwcDecimalLatitude(59.465625)
                    .withDwcDecimalLongitude(25.059035)
                    .withDwcGeodeticDatum("WGS84")
                    .withDwcCoordinateUncertaintyInMeters(10.0)
                    .withDwcCoordinatePrecision(0.0001))
                .withDwcGeologicalContext(new DwcGeologicalContext().withDwcGroup("Group")))));
  }

  private static Location givenLocationId() {
    return new Location()
        .withDwcCountry("Estonia")
        .withDwcCountryCode("EE")
        .withDwcLocationID("LOC_ID")
        .withGeoReference(new GeoReference()
            .withDwcDecimalLatitude(59.465625)
            .withDwcDecimalLongitude(25.059035))
        .withDwcGeologicalContext(new DwcGeologicalContext().withDwcGroup("Group"));
  }

  private static Location givenFootprintLocation() {
    return new Location()
        .withDwcCountry("Estonia")
        .withDwcCountryCode("EE")
        .withGeoReference(new GeoReference()
            .withDwcDecimalLatitude(59.465625)
            .withDwcDecimalLongitude(25.059035)
            .withDwcFootprintSrs("Some footprint SRS, WGS84 for example")
            .withDwcFootprintWkt("POINT (12.559220 55.702230)"))
        .withDwcGeologicalContext(new DwcGeologicalContext().withDwcGroup("Group"));
  }


  private static Location givenVerbatimCoordinateLocation() {
    return new Location()
        .withDwcLocationID("LOC_ID")
        .withGeoReference(new GeoReference()
            .withDwcVerbatimCoordinates("59.465625, 25.059035"))
        .withDwcGeologicalContext(new DwcGeologicalContext().withDwcGroup("Group"));
  }

  private static Location givenVerbatimLatLongLocation() {
    return new Location()
        .withDwcLocationID("LOC_ID")
        .withGeoReference(new GeoReference()
            .withDwcVerbatimLatitude("59.465625")
            .withDwcVerbatimLongitude("25.059035"))
        .withDwcGeologicalContext(new DwcGeologicalContext().withDwcGroup("Group"));
  }

  private static Location givenMissingUncertaintyLocation() {
    return new Location()
        .withDwcCountry("The Netherlands")
        .withGeoReference(new GeoReference()
            .withDwcDecimalLatitude(59.465625)
            .withDwcDecimalLongitude(25.059035)
            .withDwcGeodeticDatum("WGS84"))
        .withDwcGeologicalContext(new DwcGeologicalContext().withDwcGroup("Group"));
  }
  private static Location givenMissingPercisionLocation() {
    return new Location()
        .withDwcCountry("The Netherlands")
        .withGeoReference(new GeoReference()
            .withDwcDecimalLatitude(59.465625)
            .withDwcDecimalLongitude(25.059035)
            .withDwcGeodeticDatum("WGS84")
            .withDwcCoordinateUncertaintyInMeters(100.00))
        .withDwcGeologicalContext(new DwcGeologicalContext().withDwcGroup("Group"));
  }

  private static DigitalSpecimen givenMidsThreeGenericSpecimen(Location location) {
    return baseDigitalSpecimen().withDwcPreparations("single specimen")
        .withOdsMarkedAsType(true)
        .withOdsTopicDiscipline(OdsTopicDiscipline.PALAEONTOLOGY)
        .withDwcIdentification(List.of(new Identifications().withDwcIdentificationID("ID1")
            .withTaxonIdentifications(
                List.of(new TaxonIdentification().withDwcScientificNameId("ID2")))))
        .withDwcOccurrence(List.of(new Occurrences().withDctermsLocation(location)));
  }

  private static DigitalSpecimen givenFullPaleoSpecimen() {
    return baseDigitalSpecimen().withDwcPreparations("single specimen")
        .withOdsMarkedAsType(false)
        .withOdsTopicDiscipline(OdsTopicDiscipline.PALAEONTOLOGY)
        .withDwcOccurrence(List.of(new Occurrences().withDctermsLocation(
            new Location()
                .withDwcCountry("Estonia")
                .withDwcCountryCode("EE")
                .withGeoReference(new GeoReference()
                    .withDwcDecimalLatitude(59.465625)
                    .withDwcDecimalLongitude(25.059035))
                .withDwcGeologicalContext(new DwcGeologicalContext().withDwcGroup("Group")))));
  }

  private static DigitalSpecimen givenBotanySpecimen(Boolean hasMedia) {
    return baseDigitalSpecimen().withDwcPreparations("in alcohol")
        .withOdsTopicDiscipline(OdsTopicDiscipline.BOTANY)
        .withDwcRecordedBy("sam Leeflang")
        .withOdsMarkedAsType(true)
        .withDwcOccurrence(List.of(
            new Occurrences()
                .withDwcEventDate("22-09-2023")
                .withDwcFieldNumber("A field number")
                .withDctermsLocation(
                    new Location().withDwcCountry("Estonia").withDwcCountryCode("EE")
                        .withGeoReference(new GeoReference()
                            .withDwcDecimalLatitude(59.465625)
                            .withDwcDecimalLongitude(25.059035)))))
        .withOdsHasMedia(hasMedia);
  }

  private static DigitalSpecimen givenInvalidType() {
    return baseDigitalSpecimen()
        .withDwcPreparations("single specimen")
        .withOdsTopicDiscipline(OdsTopicDiscipline.UNCLASSIFIED);
  }

  private static DigitalSpecimen givenMissingCountry() {
    return baseDigitalSpecimen()
        .withDwcPreparations("single specimen")
        .withOdsMarkedAsType(true)
        .withOdsTopicDiscipline(OdsTopicDiscipline.PALAEONTOLOGY)
        .withDwcOccurrence(List.of(new Occurrences().withDctermsLocation(
            new Location()
                .withGeoReference(new GeoReference()
                    .withDwcDecimalLatitude(59.465625)
                    .withDwcDecimalLongitude(25.059035))
                .withDwcGeologicalContext(
                    new DwcGeologicalContext().withDwcEarliestEonOrLowestEonothem("Archean")))));
  }

  private static DigitalSpecimen givenMissingTopic() {
    return baseDigitalSpecimen()
        .withDwcPreparations("alcohol")
        .withDctermsModified("22-05-2024")
        .withDctermsLicense("A license")
        .withOdsSpecimenName("A nice name")
        .withOdsTopicDiscipline(null);
  }

  private static DigitalSpecimen givenPreparationValue(
      String preparationValue) {
    return baseDigitalSpecimen()
        .withDwcPreparations(preparationValue)
        .withOdsTopicDiscipline(OdsTopicDiscipline.PALAEONTOLOGY);
  }

  private static DigitalSpecimen baseDigitalSpecimen() {
    return new DigitalSpecimen()
        .withOdsPhysicalSpecimenIdType(PHYSICAL_SPECIMEN_TYPE)
        .withDwcInstitutionId(ORGANISATION_ID)
        .withOdsSpecimenName(SPECIMEN_NAME)
        .withOdsSourceSystem(SOURCE_SYSTEM_ID)
        .withDctermsModified("2017-09-26T12:27:21.000+00:00")
        .withDctermsLicense("http://creativecommons.org/licenses/by-nc/4.0/");
  }

  private static DigitalSpecimen givenMissingLongitude() {
    return baseDigitalSpecimen()
        .withDwcPreparations("single specimen")
        .withOdsMarkedAsType(true)
        .withOdsTopicDiscipline(OdsTopicDiscipline.PALAEONTOLOGY)
        .withDwcOccurrence(List.of(new Occurrences().withDctermsLocation(
            new Location().withDwcCountry("Estonia").withDwcCountryCode("EE")
                .withGeoReference(new GeoReference().withDwcDecimalLatitude(59.465625))
                .withDwcGeologicalContext(new DwcGeologicalContext().withDwcGroup("Group")))));
  }

  @BeforeEach
  void setup() {
    midsService = new MidsService();
  }

  @ParameterizedTest
  @MethodSource("provideDigitalSpecimen")
  void testCalculateMids(DigitalSpecimenWrapper digitalSpecimen, int midsLevel) {
    // Given

    // When
    var result = midsService.calculateMids(digitalSpecimen);

    // Then
    assertThat(result).isEqualTo(midsLevel);
  }
}
