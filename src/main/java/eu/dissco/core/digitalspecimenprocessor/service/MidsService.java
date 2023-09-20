package eu.dissco.core.digitalspecimenprocessor.service;

import eu.dissco.core.digitalspecimenprocessor.domain.DigitalSpecimen;
import eu.dissco.core.digitalspecimenprocessor.schema.DigitalSpecimen.OdsTopicDiscipline;
import eu.dissco.core.digitalspecimenprocessor.schema.GeologicalContext;
import eu.dissco.core.digitalspecimenprocessor.schema.Georeference;
import eu.dissco.core.digitalspecimenprocessor.schema.Location;
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
      OdsTopicDiscipline.ASTROGEOLOGY, OdsTopicDiscipline.EARTH_GEOLOGY,
      OdsTopicDiscipline.ENVIRONMENT);

  private static boolean isInvalid(String value) {
    return value == null || value.trim().isEmpty() || value.equalsIgnoreCase("null");
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
    return isInvalid(attributes.getDctermsLicense())
        || isInvalid(attributes.getDctermsModified())
        || isInvalid(attributes.getDwcPreparations())
        || isInvalid(attributes.getOdsPhysicalSpecimenIdType().value())
        || isInvalid(attributes.getOdsSpecimenName());
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
    return isInvalid(attributes.getDwcTypeStatus())
        || attributes.getOdsHasMedia()
        || isQuantitativeLocationInvalid(attributes.getOccurrences().get(0).getLocation()
        .getGeoreference())
        || isQualitativeLocationInvalid(attributes.getOccurrences().get(0).getLocation())
        || isInvalid(attributes.getOccurrences().get(0).getDwcEventDate())
        || isInvalid(attributes.getOccurrences().get(0).getDwcFieldNumber())
        || isInvalid(attributes.getDwcRecordedBy());
  }

  private boolean compliesToMidsTwoPaleoBio(
      eu.dissco.core.digitalspecimenprocessor.schema.DigitalSpecimen attributes) {
    return isInvalid(attributes.getDwcTypeStatus())
        || isStratigraphyInvalid(
        attributes.getOccurrences().get(0).getLocation().getGeologicalContext())
        || isQualitativeLocationInvalid(attributes.getOccurrences().get(0).getLocation())
        || isQuantitativeLocationInvalid(
        attributes.getOccurrences().get(0).getLocation().getGeoreference());
  }

  private boolean isQuantitativeLocationInvalid(Georeference georeference) {
    return georeference.getDwcDecimalLatitude() != null
        || georeference.getDwcDecimalLongitude() != null
        || isInvalid(georeference.getDwcGeodeticDatum());
  }

  private boolean isQualitativeLocationInvalid(Location location) {
    return isInvalid(location.getDwcContinent())
        && isInvalid(location.getDwcCountry())
        && isInvalid(location.getDwcCountryCode())
        && isInvalid(location.getDwcCounty())
        && isInvalid(location.getDwcIsland())
        && isInvalid(location.getDwcIslandGroup())
        && isInvalid(location.getDwcLocality())
        && isInvalid(location.getDwcMunicipality())
        && isInvalid(location.getDwcStateProvince())
        && isInvalid(location.getDwcWaterBody());
  }

  private boolean isStratigraphyInvalid(GeologicalContext geologicalContext) {
    return isInvalid(geologicalContext.getDwcBed())
        && isInvalid(geologicalContext.getDwcMember())
        && isInvalid(geologicalContext.getDwcFormation())
        && isInvalid(geologicalContext.getDwcGroup())
        && isInvalid(geologicalContext.getDwcLithostratigraphicTerms())
        && isInvalid(geologicalContext.getDwcHighestBiostratigraphicZone())
        && isInvalid(geologicalContext.getDwcLowestBiostratigraphicZone())
        && isInvalid(geologicalContext.getDwcLatestAgeOrHighestStage())
        && isInvalid(geologicalContext.getDwcEarliestAgeOrLowestStage())
        && isInvalid(geologicalContext.getDwcLatestEpochOrHighestSeries())
        && isInvalid(geologicalContext.getDwcEarliestEpochOrLowestSeries())
        && isInvalid(geologicalContext.getDwcLatestPeriodOrHighestSystem())
        && isInvalid(geologicalContext.getDwcEarliestPeriodOrLowestSystem())
        && isInvalid(geologicalContext.getDwcLatestEraOrHighestErathem())
        && isInvalid(geologicalContext.getDwcEarliestEraOrLowestErathem())
        && isInvalid(geologicalContext.getDwcLatestEonOrHighestEonothem())
        && isInvalid(geologicalContext.getDwcEarliestEonOrLowestEonothem());
  }


}
