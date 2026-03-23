package eu.dissco.core.digitalspecimenprocessor.controller;

import static eu.dissco.core.digitalspecimenprocessor.utils.AnnotationTestUtils.givenAnnotation;
import static org.assertj.core.api.Assertions.assertThat;

import eu.dissco.core.digitalspecimenprocessor.service.DigitalSpecimenService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;

@ExtendWith(MockitoExtension.class)
class AnnotationControllerTest {

	@Mock
	private DigitalSpecimenService digitalSpecimenService;

	private AnnotationController annotationController;

	@BeforeEach
	void setup() {
		annotationController = new AnnotationController(digitalSpecimenService);
	}

	@Test
	void testApplyAnnotation() throws Exception {
		// Given

		// When
		var result = annotationController.applyAnnotation(givenAnnotation());

		// Then
		assertThat(result.getStatusCode()).isEqualTo(HttpStatus.OK);
	}

}
