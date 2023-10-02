package eu.dissco.core.digitalspecimenprocessor.service;

import eu.dissco.core.digitalspecimenprocessor.domain.DigitalSpecimen;
import eu.dissco.core.digitalspecimenprocessor.schema.DigitalSpecimen.OdsTopicDiscipline;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class MidsService {

  private static final List<OdsTopicDiscipline> BIO_DISCIPLINES = List.of(OdsTopicDiscipline.BOTANY,
      OdsTopicDiscipline.MICROBIOLOGY, OdsTopicDiscipline.ZOOLOGY);

  private static final List<OdsTopicDiscipline> PALEO_GEO_DISCIPLINES = List.of(
      OdsTopicDiscipline.PALAEONTOLOGY,
      OdsTopicDiscipline.ASTROGEOLOGY, OdsTopicDiscipline.GEOLOGY,
      OdsTopicDiscipline.ECOLOGY);

  private static boolean isValid(String value) {
    return value != null && !value.trim().isEmpty() && !value.equalsIgnoreCase("null");
  }

  public int calculateMids(DigitalSpecimen digitalSpecimen) {
    var attributes = digitalSpecimen.attributes();
    if (!compliesToMidsOne(attributes)) {
      return 0;
    }
    if (!compliesToMidsTwo(attributes)) {
      return 1;
    }
    return 2;
  }

  private boolean compliesToMidsOne(
      eu.dissco.core.digitalspecimenprocessor.schema.DigitalSpecimen attributes) {
    return isValid(attributes.getDctermsLicense())
        && isValid(attributes.getDctermsModified())
        && isValid(attributes.getDwcPreparations())
        && isValid(attributes.getOdsPhysicalSpecimenIdType().value())
        && isValid(attributes.getOdsSpecimenName());
  }

  private boolean compliesToMidsTwo(
      eu.dissco.core.digitalspecimenprocessor.schema.DigitalSpecimen attributes) {
    if (BIO_DISCIPLINES.contains(attributes.getOdsTopicDiscipline())) {
      return compliesToMidsTwoBio(attributes);
    } else if (PALEO_GEO_DISCIPLINES.contains(attributes.getOdsTopicDiscipline())) {
      return compliesToMidsTwoPaleoBio(attributes);
    } else {
      log.warn("Digital Specimen has unknown topicDiscipline: {} level 1 is highest achievable",
          attributes.getOdsTopicDiscipline());
      return false;
    }
  }

  private boolean compliesToMidsTwoBio(
      eu.dissco.core.digitalspecimenprocessor.schema.DigitalSpecimen attributes) {
    return (attributes.getOdsMarkedAsType() != null && attributes.getOdsMarkedAsType())
        && (attributes.getOdsHasMedia() != null && attributes.getOdsHasMedia())
        && isQualitativeLocationValid(attributes)
        && isQuantitativeLocationInvalid(attributes)
        && (missingOccurrence(attributes) || isValid(
        (attributes.getOccurrences().get(0).getDwcEventDate())))
        && (missingOccurrence(attributes) || isValid(
        attributes.getOccurrences().get(0).getDwcFieldNumber()))
        && isValid(attributes.getDwcRecordedBy());
  }

  private boolean missingOccurrence(
      eu.dissco.core.digitalspecimenprocessor.schema.DigitalSpecimen attributes) {
    return attributes.getOccurrences() == null || attributes.getOccurrences().isEmpty()
        || attributes.getOccurrences().get(0) == null;
  }

  private boolean compliesToMidsTwoPaleoBio(
      eu.dissco.core.digitalspecimenprocessor.schema.DigitalSpecimen attributes) {
    return (attributes.getOdsMarkedAsType() != null && attributes.getOdsMarkedAsType())
        && isStratigraphyValid(attributes)
        && isQualitativeLocationValid(attributes)
        && isQuantitativeLocationInvalid(attributes);
  }

  private boolean isQuantitativeLocationInvalid(
      eu.dissco.core.digitalspecimenprocessor.schema.DigitalSpecimen digitalSpecimen) {
    if (digitalSpecimen.getOccurrences() != null && !digitalSpecimen.getOccurrences().isEmpty()
        && digitalSpecimen.getOccurrences().get(0).getLocation() != null
        && digitalSpecimen.getOccurrences().get(0).getLocation().getGeoreference() != null) {
      var georeference = digitalSpecimen.getOccurrences().get(0).getLocation().getGeoreference();
      return georeference.getDwcDecimalLatitude() != null
          && georeference.getDwcDecimalLongitude() != null;
    }
    return false;
  }

  private boolean isQualitativeLocationValid(
      eu.dissco.core.digitalspecimenprocessor.schema.DigitalSpecimen digitalSpecimen) {
    if (digitalSpecimen.getOccurrences() != null && !digitalSpecimen.getOccurrences().isEmpty()
        && digitalSpecimen.getOccurrences().get(0).getLocation() != null) {
      var location = digitalSpecimen.getOccurrences().get(0).getLocation();
      return isValid(location.getDwcContinent())
          || isValid(location.getDwcCountry())
          || isValid(location.getDwcCountryCode())
          || isValid(location.getDwcCounty())
          || isValid(location.getDwcIsland())
          || isValid(location.getDwcIslandGroup())
          || isValid(location.getDwcLocality())
          || isValid(location.getDwcMunicipality())
          || isValid(location.getDwcStateProvince())
          || isValid(location.getDwcWaterBody());
    }
    return false;
  }

  private boolean isStratigraphyValid(
      eu.dissco.core.digitalspecimenprocessor.schema.DigitalSpecimen digitalSpecimen) {
    if (digitalSpecimen.getOccurrences() != null && !digitalSpecimen.getOccurrences().isEmpty()
        && digitalSpecimen.getOccurrences().get(0).getLocation() != null
        && digitalSpecimen.getOccurrences().get(0).getLocation().getGeologicalContext() != null) {
      var geologicalContext = digitalSpecimen.getOccurrences().get(0).getLocation()
          .getGeologicalContext();
      return isValid(geologicalContext.getDwcBed())
          || isValid(geologicalContext.getDwcMember())
          || isValid(geologicalContext.getDwcFormation())
          || isValid(geologicalContext.getDwcGroup())
          || isValid(geologicalContext.getDwcLithostratigraphicTerms())
          || isValid(geologicalContext.getDwcHighestBiostratigraphicZone())
          || isValid(geologicalContext.getDwcLowestBiostratigraphicZone())
          || isValid(geologicalContext.getDwcLatestAgeOrHighestStage())
          || isValid(geologicalContext.getDwcEarliestAgeOrLowestStage())
          || isValid(geologicalContext.getDwcLatestEpochOrHighestSeries())
          || isValid(geologicalContext.getDwcEarliestEpochOrLowestSeries())
          || isValid(geologicalContext.getDwcLatestPeriodOrHighestSystem())
          || isValid(geologicalContext.getDwcEarliestPeriodOrLowestSystem())
          || isValid(geologicalContext.getDwcLatestEraOrHighestErathem())
          || isValid(geologicalContext.getDwcEarliestEraOrLowestErathem())
          || isValid(geologicalContext.getDwcLatestEonOrHighestEonothem())
          || isValid(geologicalContext.getDwcEarliestEonOrLowestEonothem());
    }
    return false;
  }


}
