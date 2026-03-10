package eu.dissco.core.digitalspecimenprocessor.service;

import static eu.dissco.core.digitalspecimenprocessor.util.DigitalObjectUtils.DOI_PROXY;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenRecord;
import eu.dissco.core.digitalspecimenprocessor.repository.AnnotationRepository;
import eu.dissco.core.digitalspecimenprocessor.schema.DigitalSpecimen;
import io.github.dissco.annotationlogic.validator.AnnotationValidator;
import io.github.dissco.core.annotationlogic.schema.Annotation;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class AnnotationService {

  private final AnnotationRepository annotationRepository;
  private final AnnotationValidator annotationValidator;
  private final ObjectMapper objectMapper;


  public Map<String, DigitalSpecimenRecord> applyAnnotationsForSpecimen(
      Set<DigitalSpecimenRecord> digitalSpecimenRecords) {
    var acceptedAnnotations = getAnnotationsForSpecimen(digitalSpecimenRecords);

  }

  private Map<String, List<Annotation>> getAnnotationsForSpecimen(Set<DigitalSpecimenRecord> digitalSpecimenRecords){
    var targetIdsWithProxy = digitalSpecimenRecords.stream()
        .map(DigitalSpecimenRecord::id)
        .map(id -> DOI_PROXY + id)
        .collect(Collectors.toSet());
    return annotationRepository.getAcceptedAnnotationsForObject(targetIdsWithProxy);
  }

  private DigitalSpecimen applyAnnotation(DigitalSpecimen digitalSpecimen, Annotation annotation) {
    return null;
  }

}
