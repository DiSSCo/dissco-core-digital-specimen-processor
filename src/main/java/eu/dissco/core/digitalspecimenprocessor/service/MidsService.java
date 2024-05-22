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
    if (!compliesToMidsThree(attributes)) {
      return 2;
    }
    return 3;
  }

  private boolean compliesToMidsOne(DigitalSpecimen attributes) {
    return isValid(attributes.getDctermsLicense())
        && isValid(attributes.getDctermsModified())
        && isValid(attributes.getDwcPreparations())
        && isValid(attributes.getOdsTopicDiscipline().value())
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

  private boolean compliesToMidsThree(DigitalSpecimen attributes) {
    var topComplies = hasValue(attributes.getDwcInstitutionId());
    var taxValuesComply = taxValuesComply(attributes);
    var geographicalValuesComply = geographicalValuesComply(attributes);
    if (PALEO_GEO_DISCIPLINES.contains(attributes.getOdsTopicDiscipline())) {
      return topComplies && taxValuesComply && geographicalValuesComply;
    } else {
      return topComplies && taxValuesComply && geographicalValuesComply && isValid(
          attributes.getDwcRecordedById());
    }
  }

  private boolean geographicalValuesComply(DigitalSpecimen attributes) {
    if (attributes.getDwcOccurrence() != null
        && attributes.getDwcOccurrence().get(0).getDctermsLocation() != null) {
      var location = attributes.getDwcOccurrence().get(0).getDctermsLocation();
      if (isValid(location.getDwcLocationID())) {
        return true;
      } else if (location.getGeoReference() != null) {
        var georeference = location.getGeoReference();
        return (georeference.getDwcDecimalLatitude() != null
            && georeference.getDwcDecimalLongitude() != null
            && isValid(georeference.getDwcGeodeticDatum())
            && georeference.getDwcCoordinateUncertaintyInMeters() != null
            && georeference.getDwcCoordinatePrecision() != null)
            || (isValid(georeference.getDwcFootprintWkt())
            && isValid(georeference.getDwcFootprintSrs()));
      }
    }
    return false;
  }

  private boolean taxValuesComply(DigitalSpecimen attributes) {
    var hasScientificNameId = false;
    var hasIdentifiedById = false;
    for (var identifications : attributes.getDwcIdentification()) {
      if (isValid(identifications.getDwcIdentificationID())) {
        hasIdentifiedById = true;
      }
      for (var taxonIdentification : identifications.getTaxonIdentifications()) {
        if (isValid(taxonIdentification.getDwcScientificNameId())) {
          hasScientificNameId = true;
        }
      }
    }
    return hasScientificNameId && hasIdentifiedById;
  }

  private boolean compliesToMidsTwoBio(DigitalSpecimen attributes) {
    return attributes.getOdsMarkedAsType() != null
        && (attributes.getOdsHasMedia() != null && attributes.getOdsHasMedia())
        && isQualitativeLocationValid(attributes)
        && isQuantitativeLocationValid(attributes)
        && (occurrenceIsPresent(attributes) && hasValue(
        attributes.getDwcOccurrence().get(0).getDwcEventDate(),
        attributes.getDwcOccurrence().get(0).getDwcVerbatimEventDate(),
        convertInteger(attributes.getDwcOccurrence().get(0).getDwcYear())))
        && (occurrenceIsPresent(attributes) &&
        hasValue(attributes.getDwcOccurrence().get(0).getDwcFieldNumber(),
            attributes.getDwcOccurrence().get(0).getDwcRecordNumber()))
        && hasValue(attributes.getDwcRecordedBy(), attributes.getDwcRecordedById());
  }

  private String convertInteger(Integer integer) {
    if (integer != null) {
      return integer.toString();
    } else {
      return null;
    }
  }

  private boolean hasValue(String... terms) {
    for (var term : terms) {
      if (isValid(term)) {
        return true;
      }
    }
    return false;
  }


  private boolean compliesToMidsTwoPaleoBio(DigitalSpecimen attributes) {
    return attributes.getOdsMarkedAsType() != null
        && isStratigraphyValid(attributes)
        && isQualitativeLocationValid(attributes)
        && isQuantitativeLocationValid(attributes);
  }

  private boolean isQuantitativeLocationValid(DigitalSpecimen digitalSpecimen) {
    if (locationIsPresent(digitalSpecimen)
        && digitalSpecimen.getDwcOccurrence().get(0).getDctermsLocation().getGeoReference()
        != null) {
      var location = digitalSpecimen.getDwcOccurrence().get(0).getDctermsLocation();
      var georeference = location.getGeoReference();
      var validValue = hasValue(location.getDwcLocationID(), georeference.getDwcFootprintWkt());
      if (!validValue) {
        validValue = georeference.getDwcDecimalLatitude() != null
            && georeference.getDwcDecimalLongitude() != null;
      }
      return validValue;
    }
    return false;
  }

  private boolean occurrenceIsPresent(DigitalSpecimen attributes) {
    return attributes.getDwcOccurrence() != null && !attributes.getDwcOccurrence().isEmpty()
        && attributes.getDwcOccurrence().get(0) != null;
  }

  private boolean locationIsPresent(DigitalSpecimen digitalSpecimen) {
    return occurrenceIsPresent(digitalSpecimen)
        && digitalSpecimen.getDwcOccurrence().get(0).getDctermsLocation() != null;
  }

  private boolean isQualitativeLocationValid(DigitalSpecimen digitalSpecimen) {
    if (locationIsPresent(digitalSpecimen)) {
      var validValue = false;
      var location = digitalSpecimen.getDwcOccurrence().get(0).getDctermsLocation();
      validValue = hasValue(
          location.getDwcContinent(),
          location.getDwcCountry(),
          location.getDwcCountryCode(),
          location.getDwcCounty(),
          location.getDwcIsland(),
          location.getDwcIslandGroup(),
          location.getDwcLocality(),
          location.getDwcVerbatimLocality(),
          location.getDwcMunicipality(),
          location.getDwcStateProvince(),
          location.getDwcWaterBody(),
          location.getDwcHigherGeography(),
          location.getGeoReference().getDwcVerbatimCoordinates());
      if (!validValue && (location.getGeoReference() != null)) {
        validValue = isValid(location.getGeoReference().getDwcVerbatimCoordinates());
        if (!validValue) {
          validValue = (isValid(location.getGeoReference().getDwcVerbatimLatitude()) && isValid(
              location.getGeoReference().getDwcVerbatimLongitude()));
        }
      }
      return validValue;
    }
    return false;
  }

  private boolean isStratigraphyValid(DigitalSpecimen digitalSpecimen) {
    if (locationIsPresent(digitalSpecimen)
        && digitalSpecimen.getDwcOccurrence().get(0).getDctermsLocation().getDwcGeologicalContext()
        != null) {
      var geologicalContext = digitalSpecimen.getDwcOccurrence().get(0).getDctermsLocation()
          .getDwcGeologicalContext();
      return hasValue(geologicalContext.getDwcBed(),
          geologicalContext.getDwcMember(),
          geologicalContext.getDwcFormation(),
          geologicalContext.getDwcGroup(),
          geologicalContext.getDwcLithostratigraphicTerms(),
          geologicalContext.getDwcHighestBiostratigraphicZone(),
          geologicalContext.getDwcLowestBiostratigraphicZone(),
          geologicalContext.getDwcLatestAgeOrHighestStage(),
          geologicalContext.getDwcEarliestAgeOrLowestStage(),
          geologicalContext.getDwcLatestEpochOrHighestSeries(),
          geologicalContext.getDwcEarliestEpochOrLowestSeries(),
          geologicalContext.getDwcLatestPeriodOrHighestSystem(),
          geologicalContext.getDwcEarliestPeriodOrLowestSystem(),
          geologicalContext.getDwcLatestEraOrHighestErathem(),
          geologicalContext.getDwcEarliestEraOrLowestErathem(),
          geologicalContext.getDwcLatestEonOrHighestEonothem(),
          geologicalContext.getDwcEarliestEonOrLowestEonothem());
    }
    return false;
  }

}
