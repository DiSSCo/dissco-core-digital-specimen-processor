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
        && attributes.getOdsTopicDiscipline() != null
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
    if (PALEO_GEO_DISCIPLINES.contains(attributes.getOdsTopicDiscipline())) {
      return checkMidsThreeGeneric(attributes);
    } else {
      return checkMidsThreeGeneric(attributes) && isValid(attributes.getDwcRecordedByID());
    }
  }

  private boolean checkMidsThreeGeneric(DigitalSpecimen attributes) {
    return hasValue(attributes.getOdsOrganisationID()) && taxValuesComply(attributes)
        && geographicalValuesComply(attributes);
  }

  private boolean geographicalValuesComply(DigitalSpecimen attributes) {
    if (attributes.getOdsHasEvent() != null
        && attributes.getOdsHasEvent().get(0).getOdsLocation() != null) {
      var location = attributes.getOdsHasEvent().get(0).getOdsLocation();
      if (isValid(location.getDwcLocationID())) {
        return true;
      } else if (location.getOdsGeoReference() != null) {
        var geoReference = location.getOdsGeoReference();
        return (geoReference.getDwcDecimalLatitude() != null
            && geoReference.getDwcDecimalLongitude() != null
            && isValid(geoReference.getDwcGeodeticDatum())
            && geoReference.getDwcCoordinateUncertaintyInMeters() != null
            && geoReference.getDwcCoordinatePrecision() != null)
            || (isValid(geoReference.getDwcFootprintWKT())
            && isValid(geoReference.getDwcFootprintSRS()));
      }
    }
    return false;
  }

  private boolean taxValuesComply(DigitalSpecimen attributes) {
    var hasScientificNameId = false;
    var hasIdentifiedById = false;
    for (var identifications : attributes.getOdsHasIdentification()) {
      if (isValid(identifications.getDwcIdentifiedByID())) {
        hasIdentifiedById = true;
      }
      for (var taxonIdentification : identifications.getOdsHasTaxonIdentification()) {
        if (isValid(taxonIdentification.getDwcScientificNameID())) {
          hasScientificNameId = true;
        }
      }
    }
    return hasScientificNameId && hasIdentifiedById;
  }

  private boolean compliesToMidsTwoBio(DigitalSpecimen attributes) {
    return attributes.getOdsIsMarkedAsType() != null
        && Boolean.TRUE.equals(attributes.getOdsIsKnownToContainMedia())
        && qualitativeLocationIsValid(attributes)
        && quantitativeLocationIsValid(attributes)
        && (eventIsPresent(attributes) && hasValue(
        attributes.getOdsHasEvent().get(0).getDwcEventDate(),
        attributes.getOdsHasEvent().get(0).getDwcVerbatimEventDate(),
        convertInteger(attributes.getOdsHasEvent().get(0).getDwcYear())))
        && (eventIsPresent(attributes) &&
        hasValue(attributes.getOdsHasEvent().get(0).getDwcFieldNumber(),
            attributes.getOdsHasEvent().get(0).getDwcRecordNumber()))
        && hasValue(attributes.getDwcRecordedBy(), attributes.getDwcRecordedByID());
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
    return attributes.getOdsIsMarkedAsType() != null
        && stratigraphyIsValid(attributes)
        && qualitativeLocationIsValid(attributes)
        && quantitativeLocationIsValid(attributes);
  }

  private boolean quantitativeLocationIsValid(DigitalSpecimen digitalSpecimen) {
    if (locationIsPresent(digitalSpecimen)
        && digitalSpecimen.getOdsHasEvent().get(0).getOdsLocation().getOdsGeoReference()
        != null) {
      var location = digitalSpecimen.getOdsHasEvent().get(0).getOdsLocation();
      var geoReference = location.getOdsGeoReference();
      return hasValue(location.getDwcLocationID(), geoReference.getDwcFootprintWKT()) || (
          geoReference.getDwcDecimalLatitude() != null
              && geoReference.getDwcDecimalLongitude() != null);
    }
    return false;
  }

  private boolean eventIsPresent(DigitalSpecimen attributes) {
    return attributes.getOdsHasEvent() != null && !attributes.getOdsHasEvent().isEmpty()
        && attributes.getOdsHasEvent().get(0) != null;
  }

  private boolean locationIsPresent(DigitalSpecimen digitalSpecimen) {
    return eventIsPresent(digitalSpecimen)
        && digitalSpecimen.getOdsHasEvent().get(0).getOdsLocation() != null;
  }

  private boolean qualitativeLocationIsValid(DigitalSpecimen digitalSpecimen) {
    if (locationIsPresent(digitalSpecimen)) {
      var isValidValue = false;
      var location = digitalSpecimen.getOdsHasEvent().get(0).getOdsLocation();
      isValidValue = hasValue(
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
          location.getDwcHigherGeography());
      if (!isValidValue && (location.getOdsGeoReference() != null)) {
        isValidValue = isValid(location.getOdsGeoReference().getDwcVerbatimCoordinates());
        if (!isValidValue) {
          isValidValue = (isValid(location.getOdsGeoReference().getDwcVerbatimLatitude()) && isValid(
              location.getOdsGeoReference().getDwcVerbatimLongitude()));
        }
      }
      return isValidValue;
    }
    return false;
  }

  private boolean stratigraphyIsValid(DigitalSpecimen digitalSpecimen) {
    if (locationIsPresent(digitalSpecimen)
        && digitalSpecimen.getOdsHasEvent().get(0).getOdsLocation().getOdsGeologicalContext()
        != null) {
      var geologicalContext = digitalSpecimen.getOdsHasEvent().get(0).getOdsLocation()
          .getOdsGeologicalContext();
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
