package eu.dissco.core.digitalspecimenprocessor.service;

import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.ANOTHER_SPECIMEN_NAME;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MAPPER;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.PHYSICAL_SPECIMEN_ID;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.UPDATED;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.UPDATED_STR;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenWrapper;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenWrapperNoOriginalData;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenWrapperWithMediaEr;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.Option;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenWrapper;
import eu.dissco.core.digitalspecimenprocessor.schema.DigitalSpecimen;
import eu.dissco.core.digitalspecimenprocessor.schema.DigitalSpecimen.OdsTopicDiscipline;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Slf4j
class EqualityServiceTest {

  EqualityService equalityService;
  Configuration jsonPathConfiguration = Configuration.builder()
      .options(Option.AS_PATH_LIST, Option.SUPPRESS_EXCEPTIONS, Option.ALWAYS_RETURN_LIST)
      .build();

  @BeforeEach
  void setup() {
    equalityService = new EqualityService(jsonPathConfiguration, MAPPER);
  }

  @ParameterizedTest
  @MethodSource("provideEqualSpecimens")
  void testEqualSpecimens(DigitalSpecimenWrapper currentDigitalSpecimen,
      DigitalSpecimenWrapper digitalSpecimen) throws JsonProcessingException {
    // When
    var result = equalityService.isEqual(currentDigitalSpecimen, digitalSpecimen);

    // Then
    assertThat(result).isTrue();
  }

  @Test
  void testUnequalSpecimens() throws JsonProcessingException {
    // Given
    var currentDigitalSpecimen = givenDigitalSpecimenWrapper();
    var digitalSpecimen = new DigitalSpecimenWrapper(PHYSICAL_SPECIMEN_ID, ANOTHER_SPECIMEN_NAME,
        new DigitalSpecimen().withOdsTopicDiscipline(OdsTopicDiscipline.ECOLOGY), null);

    // When
    var result = equalityService.isEqual(currentDigitalSpecimen, digitalSpecimen);

    // Then
    assertThat(result).isFalse();
  }

  private static Stream<Arguments> provideEqualSpecimens() {
    return Stream.of(
        Arguments.of(givenDigitalSpecimenWrapper(),
            changeTimestamps(givenDigitalSpecimenWrapper())),
        Arguments.of(givenDigitalSpecimenWrapper(true, true),
            changeTimestamps(givenDigitalSpecimenWrapper(true, true))),
        Arguments.of(givenDigitalSpecimenWrapperWithMediaEr(PHYSICAL_SPECIMEN_ID, true),
            changeTimestamps(givenDigitalSpecimenWrapper(true, true))),
        Arguments.of(givenDigitalSpecimenWrapper(),
            changeTimestamps(givenDigitalSpecimenWrapperNoOriginalData()))
    );
  }

  private static DigitalSpecimenWrapper changeTimestamps(
      DigitalSpecimenWrapper digitalSpecimenWrapper) {
    var attributes = digitalSpecimenWrapper.attributes();
    attributes
        .withDctermsCreated(UPDATED)
        .withDctermsModified(UPDATED_STR);
    if (attributes.getOdsHasEntityRelationships() != null) {
      attributes.getOdsHasEntityRelationships()
          .forEach(er -> er.setDwcRelationshipEstablishedDate(UPDATED));
    }
    return new DigitalSpecimenWrapper(digitalSpecimenWrapper.physicalSpecimenID(),
        digitalSpecimenWrapper.type(), attributes, digitalSpecimenWrapper.originalAttributes());

  }


}
