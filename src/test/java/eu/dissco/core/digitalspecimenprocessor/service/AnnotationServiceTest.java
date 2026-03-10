package eu.dissco.core.digitalspecimenprocessor.service;

import static eu.dissco.core.digitalspecimenprocessor.utils.AnnotationTestUtils.ANNOTATION_ID;
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
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenRecord;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenWrapper;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;

import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenWrapper;
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

  @BeforeEach
  void setup() {
    annotationService = new AnnotationService(annotationRepository, annotationValidator, MAPPER);
  }

  @Test
  void testGetAnnotationsForSpecimens() {
    // Given

    // When
    annotationService.getAnnotationsForSpecimens(Set.of(givenDigitalSpecimenRecord()));

    // Then
    then(annotationRepository).should()
        .getAcceptedAnnotationsForObject(Set.of(DOI_PREFIX + HANDLE));
  }

  @Test
  void testGetAnnotationsForSpecimensNoRecords() {
    // Given

    // When
    annotationService.getAnnotationsForSpecimens(Set.of());

    // Then
    then(annotationRepository).shouldHaveNoInteractions();
  }


  @Test
  void testApplyAcceptedAnnotations() throws Exception {
    // Given
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
    var result = annotationService.applyAcceptedAnnotations(givenDigitalSpecimenWrapper(), HANDLE,
        Map.of(HANDLE, List.of(givenAnnotation())));

    // Then
    assertThat(result).isEqualTo(expected);
  }

  @Test
  void testApplyAcceptedAnnotationsFails() throws Exception {
    // Given
    given(annotationValidator.applyAnnotation(any(DigitalSpecimen.class), eq(givenAnnotation())))
        .willThrow(InvalidAnnotationException.class);

    // When
    var result = annotationService.applyAcceptedAnnotations(givenDigitalSpecimenWrapper(), HANDLE,
        Map.of(HANDLE, List.of(givenAnnotation())));

    // Then
    assertThat(result).isEqualTo(givenDigitalSpecimenWrapper());
  }

  @Test
  void testMarkAnnotationsAsMerged() {
    // Given

    // When
    annotationService.markAnnotationsAsMerged(Map.of(givenDigitalSpecimenRecord(), ORIGINAL_DATA),
        Map.of(HANDLE, List.of(givenAnnotation())));

    // Then
    then(annotationRepository).should().markAnnotationsAsMerged(Set.of(ANNOTATION_ID));
  }

}
