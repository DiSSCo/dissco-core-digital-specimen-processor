package eu.dissco.core.digitalspecimenprocessor.service;

import static eu.dissco.core.digitalspecimenprocessor.util.DigitalObjectUtils.DOI_PROXY;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenEvent;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenRecord;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenWrapper;
import eu.dissco.core.digitalspecimenprocessor.property.AnnotationProperties;
import eu.dissco.core.digitalspecimenprocessor.repository.AnnotationRepository;
import eu.dissco.core.digitalspecimenprocessor.schema.DigitalSpecimen;
import io.github.dissco.annotationlogic.exception.InvalidAnnotationException;
import io.github.dissco.annotationlogic.exception.InvalidTargetException;
import io.github.dissco.annotationlogic.validator.AnnotationValidator;
import io.github.dissco.core.annotationlogic.schema.Annotation;
import java.util.Date;
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
  private final AnnotationProperties properties;

  public Map<String, List<Annotation>> getAnnotationsForSpecimens(
      Set<DigitalSpecimenRecord> digitalSpecimenRecords) {
    if (!properties.isApplyAcceptedAnnotations() || digitalSpecimenRecords.isEmpty()) {
      return Map.of();
    }
    var targetIdsWithProxy = digitalSpecimenRecords.stream()
        .map(DigitalSpecimenRecord::id)
        .map(id -> DOI_PROXY + id)
        .collect(Collectors.toSet());
    return annotationRepository.getAcceptedAnnotationsForObject(targetIdsWithProxy);
  }

  public DigitalSpecimenEvent applyAcceptedAnnotations(
      DigitalSpecimenEvent digitalSpecimenEvent,
      DigitalSpecimenRecord currentSpecimen, Map<String, List<Annotation>> acceptedAnnotations) {
    if (!properties.isApplyAcceptedAnnotations() || acceptedAnnotations.isEmpty()) {
      return digitalSpecimenEvent;
    }
    var annotationList = acceptedAnnotations.get(currentSpecimen.id());
    var digitalSpecimen = digitalSpecimenEvent.digitalSpecimenWrapper().attributes();
    for (var annotation : annotationList) {
      digitalSpecimen = applySingleAnnotation(digitalSpecimen, annotation, currentSpecimen);
    }
    return
        new DigitalSpecimenEvent(
            digitalSpecimenEvent.masList(),
            new DigitalSpecimenWrapper(
                digitalSpecimenEvent.digitalSpecimenWrapper().physicalSpecimenID(),
                digitalSpecimenEvent.digitalSpecimenWrapper().type(),
                digitalSpecimen,
                digitalSpecimenEvent.digitalSpecimenWrapper().originalAttributes()
            ),
            digitalSpecimenEvent.digitalMediaEvents(),
            digitalSpecimenEvent.forceMasSchedule(),
            digitalSpecimenEvent.isDataFromSourceSystem());
  }

  private DigitalSpecimen applySingleAnnotation(DigitalSpecimen digitalSpecimen,
      Annotation annotation, DigitalSpecimenRecord currentSpecimen) {
    var digitalSpecimenConverted = mapper.convertValue(digitalSpecimen,
            io.github.dissco.core.annotationlogic.schema.DigitalSpecimen.class)
        // Add required fields so that our annotation validator accepts the annotation
        .withDctermsIdentifier(DOI_PROXY + currentSpecimen.id())
        .withId(DOI_PROXY + currentSpecimen.id())
        .withDctermsCreated(Date.from(currentSpecimen.created()));
    try {
      digitalSpecimenConverted = annotationValidator.applyAnnotation(digitalSpecimenConverted,
          annotation);
    } catch (InvalidAnnotationException | InvalidTargetException e) {
      log.error("Unable to apply annotation {} to digital specimen. Ignoring annotation",
          annotation.getDctermsIdentifier(), e);
      return digitalSpecimen;
    }
    return mapper.convertValue(digitalSpecimenConverted, DigitalSpecimen.class);
  }

}
