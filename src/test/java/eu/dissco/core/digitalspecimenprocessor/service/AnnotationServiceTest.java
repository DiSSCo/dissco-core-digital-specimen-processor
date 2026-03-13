package eu.dissco.core.digitalspecimenprocessor.service;

import static eu.dissco.core.digitalspecimenprocessor.utils.AnnotationTestUtils.NEW_VALUE;
import static eu.dissco.core.digitalspecimenprocessor.utils.AnnotationTestUtils.givenAnnotatedSpecimen;
import static eu.dissco.core.digitalspecimenprocessor.utils.AnnotationTestUtils.givenAnnotation;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.DOI_PREFIX;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.HANDLE;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MAPPER;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.ORGANISATION_ID;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.ORIGINAL_DATA;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.PHYSICAL_SPECIMEN_ID;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.SPECIMEN_NAME;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.TYPE_PID;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenAttributes;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenEvent;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenRecord;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;

import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenWrapper;
import eu.dissco.core.digitalspecimenprocessor.exception.AnnotationProcessingException;
import eu.dissco.core.digitalspecimenprocessor.property.AnnotationProperties;
import eu.dissco.core.digitalspecimenprocessor.repository.AnnotationRepository;
import io.github.dissco.annotationlogic.exception.InvalidAnnotationException;
import io.github.dissco.annotationlogic.validator.AnnotationValidator;
import io.github.dissco.core.annotationlogic.schema.DigitalSpecimen;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class AnnotationServiceTest {

  private AnnotationService annotationService;
  @Mock
  AnnotationRepository annotationRepository;
  @Mock
  AnnotationValidator annotationValidator;
  @Mock
  AnnotationProperties annotationProperties;

  @BeforeEach
  void setup() {
    annotationService = new AnnotationService(annotationRepository, annotationValidator, MAPPER,
        annotationProperties);
  }

  @Test
  void testGetAnnotationsForSpecimens() {
    // Given
    given(annotationProperties.isApplyAcceptedAnnotations()).willReturn(true);

    // When
    annotationService.getAnnotationsForSpecimens(Set.of(givenDigitalSpecimenRecord()));

    // Then
    then(annotationRepository).should()
        .getAcceptedAnnotationsForObject(Set.of(DOI_PREFIX + HANDLE));
  }

  @Test
  void testGetAnnotationsForSpecimensNoRecords() {
    // Given
    given(annotationProperties.isApplyAcceptedAnnotations()).willReturn(true);

    // When
    annotationService.getAnnotationsForSpecimens(Set.of());

    // Then
    then(annotationRepository).shouldHaveNoInteractions();
  }


  @Test
  void testApplyAcceptedAnnotations() throws Exception {
    // Given
    given(annotationProperties.isApplyAcceptedAnnotations()).willReturn(true);
    var annotatedSpecimen = givenAnnotatedSpecimen();
    var expected = new DigitalSpecimenWrapper(
        PHYSICAL_SPECIMEN_ID,
        TYPE_PID,
        givenAttributes(SPECIMEN_NAME, ORGANISATION_ID, true, false, false)
            .withDwcOrganismRemarks(NEW_VALUE),
        ORIGINAL_DATA
    );
    given(annotationValidator.applyAnnotation(any(DigitalSpecimen.class),
        eq(givenAnnotation()))).willReturn(
        annotatedSpecimen);

    // When
    var result = annotationService.applyAcceptedAnnotations(givenDigitalSpecimenEvent(),
        givenDigitalSpecimenRecord(),
        Map.of(HANDLE, List.of(givenAnnotation())));

    // Then
    assertThat(result.digitalSpecimenWrapper()).isEqualTo(expected);
  }

  @Test
  void testApplyAcceptedAnnotationsFails() throws Exception {
    // Given
    given(annotationProperties.isApplyAcceptedAnnotations()).willReturn(true);
    given(annotationValidator.applyAnnotation(any(DigitalSpecimen.class), eq(givenAnnotation())))
        .willThrow(InvalidAnnotationException.class);

    // When / Then
    assertThrows(AnnotationProcessingException.class, () -> annotationService.applyAcceptedAnnotations(givenDigitalSpecimenEvent(),
        givenDigitalSpecimenRecord(),
        Map.of(HANDLE, List.of(givenAnnotation()))));
  }

}
