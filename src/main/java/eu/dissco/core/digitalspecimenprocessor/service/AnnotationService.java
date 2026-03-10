package eu.dissco.core.digitalspecimenprocessor.service;

import static eu.dissco.core.digitalspecimenprocessor.util.DigitalObjectUtils.DOI_PROXY;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenRecord;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenWrapper;
import eu.dissco.core.digitalspecimenprocessor.repository.AnnotationRepository;
import eu.dissco.core.digitalspecimenprocessor.schema.DigitalSpecimen;
import io.github.dissco.annotationlogic.exception.InvalidAnnotationException;
import io.github.dissco.annotationlogic.exception.InvalidTargetException;
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
  private final ObjectMapper mapper;

  public Map<String, List<Annotation>> getAnnotationsForSpecimen(
      Set<DigitalSpecimenRecord> digitalSpecimenRecords) {
    var targetIdsWithProxy = digitalSpecimenRecords.stream()
        .map(DigitalSpecimenRecord::id)
        .map(id -> DOI_PROXY + id)
        .collect(Collectors.toSet());
    return annotationRepository.getAcceptedAnnotationsForObject(targetIdsWithProxy);
  }

  public DigitalSpecimenWrapper applyAcceptedAnnotations(DigitalSpecimenWrapper digitalSpecimenWrapper,
      String specimenId, Map<String, List<Annotation>> acceptedAnnotations) {
    var annotationList = acceptedAnnotations.get(specimenId);
    var digitalSpecimen = digitalSpecimenWrapper.attributes();
    for (var annotation : annotationList) {
      digitalSpecimen = applySingleAnnotation(digitalSpecimen, annotation, specimenId);
    }
    return new DigitalSpecimenWrapper(
        digitalSpecimenWrapper.physicalSpecimenID(),
        digitalSpecimenWrapper.type(),
        digitalSpecimen,
        digitalSpecimenWrapper.originalAttributes()
    );
  }

  private DigitalSpecimen applySingleAnnotation(DigitalSpecimen digitalSpecimen,
      Annotation annotation, String specimenId) {
    var digitalSpecimenConverted = mapper.convertValue(digitalSpecimen,
            io.github.dissco.core.annotationlogic.schema.DigitalSpecimen.class)
        .withDctermsIdentifier(DOI_PROXY + specimenId)
        .withId(specimenId);
    try {
      digitalSpecimenConverted = annotationValidator.applyAnnotation(digitalSpecimenConverted,
          annotation);
    } catch (InvalidAnnotationException | InvalidTargetException e) {
      log.error("Unable to apply annotation {} to digital specimen. Ignoring annotation",
          annotation.getDctermsIdentifier(), e);
    }
    return mapper.convertValue(digitalSpecimenConverted, DigitalSpecimen.class);
  }

}
