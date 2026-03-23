package eu.dissco.core.digitalspecimenprocessor.controller;


import eu.dissco.core.digitalspecimenprocessor.Profiles;
import eu.dissco.core.digitalspecimenprocessor.exception.AnnotationProcessingException;
import eu.dissco.core.digitalspecimenprocessor.exception.PidException;
import eu.dissco.core.digitalspecimenprocessor.service.DigitalSpecimenService;
import io.github.dissco.core.annotationlogic.schema.Annotation;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@Profile(Profiles.WEB)
@RestController
@RequestMapping("/annotation")
@RequiredArgsConstructor
public class AnnotationController {

  private final DigitalSpecimenService digitalSpecimenService;

  @PostMapping(value = "", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<Void> applyAnnotation(@RequestBody Annotation annotation) throws PidException, AnnotationProcessingException {
    log.info("Received request to apply annotation {} to its target", annotation.getDctermsIdentifier());
    digitalSpecimenService.applyAnnotation(annotation);
    return ResponseEntity.status(HttpStatus.OK).body(null);
  }


}
