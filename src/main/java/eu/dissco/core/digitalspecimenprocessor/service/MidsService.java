package eu.dissco.core.digitalspecimenprocessor.service;

import eu.dissco.core.digitalspecimenprocessor.domain.DigitalSpecimenWrapper;
import eu.dissco.core.digitalspecimenprocessor.schema.DigitalSpecimen;
import eu.dissco.core.digitalspecimenprocessor.schema.DigitalSpecimen.OdsTopicDiscipline;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class MidsService {

  private static final List<OdsTopicDiscipline> BIO_DISCIPLINES = List.of(OdsTopicDiscipline.BOTANY,
      OdsTopicDiscipline.MICROBIOLOGY, OdsTopicDiscipline.ZOOLOGY,
      OdsTopicDiscipline.OTHER_BIODIVERSITY);

  private static final List<OdsTopicDiscipline> PALEO_GEO_DISCIPLINES = List.of(
      OdsTopicDiscipline.PALAEONTOLOGY, OdsTopicDiscipline.ASTROGEOLOGY, OdsTopicDiscipline.GEOLOGY,
      OdsTopicDiscipline.ECOLOGY, OdsTopicDiscipline.OTHER_GEODIVERSITY);

  private static boolean isValid(String value) {
    return value != null && !value.trim().isEmpty() && !value.equalsIgnoreCase("null");
  }

  public int calculateMids(DigitalSpecimenWrapper digitalSpecimenWrapper) {
    var attributes = digitalSpecimenWrapper.attributes();
    if (!compliesToMidsOne(attributes)) {
      return 0;
    }
    if (!compliesToMidsTwo(attributes)) {
      return 1;
    }
    return 2;
  }

  private boolean compliesToMidsOne(DigitalSpecimen attributes) {
    return isValid(attributes.getDctermsLicense())
        && isValid(attributes.getDctermsModified())
        && isValid(attributes.getDwcPreparations())
        && isValid(attributes.getOdsPhysicalSpecimenIdType().value())
        && isValid(attributes.getOdsSpecimenName());
  }

  private boolean compliesToMidsTwo(DigitalSpecimen attributes) {
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

  private boolean compliesToMidsTwoBio(DigitalSpecimen attributes) {
    return (attributes.getOdsMarkedAsType() != null && attributes.getOdsMarkedAsType())
        && (attributes.getOdsHasMedia() != null && attributes.getOdsHasMedia())
        && isQualitativeLocationValid(attributes)
        && isQuantitativeLocationValid(attributes)
        && (occurrenceIsPresent(attributes) && isValid(
        (attributes.getOccurrences().get(0).getDwcEventDate())))
        && (occurrenceIsPresent(attributes) && isValid(
        attributes.getOccurrences().get(0).getDwcFieldNumber()))
        && isValid(attributes.getDwcRecordedBy());
  }

  private boolean occurrenceIsPresent(DigitalSpecimen attributes) {
    return attributes.getOccurrences() != null && !attributes.getOccurrences().isEmpty()
        && attributes.getOccurrences().get(0) != null;
  }

  private boolean compliesToMidsTwoPaleoBio(DigitalSpecimen attributes) {
    return (attributes.getOdsMarkedAsType() != null && attributes.getOdsMarkedAsType())
        && isStratigraphyValid(attributes)
        && isQualitativeLocationValid(attributes)
        && isQuantitativeLocationValid(attributes);
  }

  private boolean isQuantitativeLocationValid(DigitalSpecimen digitalSpecimen) {
    if (locationIsPresent(digitalSpecimen)
        && digitalSpecimen.getOccurrences().get(0).getLocation().getGeoreference() != null) {
      var georeference = digitalSpecimen.getOccurrences().get(0).getLocation().getGeoreference();
      return georeference.getDwcDecimalLatitude() != null
          && georeference.getDwcDecimalLongitude() != null;
    }
    return false;
  }

  private boolean locationIsPresent(DigitalSpecimen digitalSpecimen) {
    return occurrenceIsPresent(digitalSpecimen)
        && digitalSpecimen.getOccurrences().get(0).getLocation() != null;
  }

  private boolean isQualitativeLocationValid(DigitalSpecimen digitalSpecimen) {
    if (locationIsPresent(digitalSpecimen)) {
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

  private boolean isStratigraphyValid(DigitalSpecimen digitalSpecimen) {
    if (locationIsPresent(digitalSpecimen)
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
