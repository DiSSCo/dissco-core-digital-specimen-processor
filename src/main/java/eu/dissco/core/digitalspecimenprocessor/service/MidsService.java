package eu.dissco.core.digitalspecimenprocessor.service;

import static eu.dissco.core.digitalspecimenprocessor.domain.AgentRoleType.COLLECTOR;
import static eu.dissco.core.digitalspecimenprocessor.domain.AgentRoleType.IDENTIFIER;

import eu.dissco.core.digitalspecimenprocessor.domain.AgentRoleType;
import eu.dissco.core.digitalspecimenprocessor.schema.Agent;
import eu.dissco.core.digitalspecimenprocessor.schema.DigitalSpecimen;
import eu.dissco.core.digitalspecimenprocessor.schema.DigitalSpecimen.OdsTopicDiscipline;
import eu.dissco.core.digitalspecimenprocessor.schema.Event;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class MidsService {

  private static final List<String> UNACCEPTED_VALUES = List.of("null", "unknown",
      "unknown:undigitized");

  private static final List<OdsTopicDiscipline> BIO_DISCIPLINES = List.of(OdsTopicDiscipline.BOTANY,
      OdsTopicDiscipline.MICROBIOLOGY, OdsTopicDiscipline.ZOOLOGY,
      OdsTopicDiscipline.OTHER_BIODIVERSITY);

  private static final List<OdsTopicDiscipline> PALEO_GEO_DISCIPLINES = List.of(
      OdsTopicDiscipline.PALAEONTOLOGY, OdsTopicDiscipline.ASTROGEOLOGY, OdsTopicDiscipline.GEOLOGY,
      OdsTopicDiscipline.ECOLOGY, OdsTopicDiscipline.OTHER_GEODIVERSITY);

  private static boolean isValid(String value) {
    if (value == null || value.trim().isEmpty()) {
      return false;
    }
    return !UNACCEPTED_VALUES.contains(value.toLowerCase());
  }

  public int calculateMids(
      eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenWrapper digitalSpecimenWrapper) {
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
      return checkMidsThreeGeneric(attributes) && hasAgentWithRole(COLLECTOR,
          attributes.getOdsHasEvents().stream()
              .map(Event::getOdsHasAgents).flatMap(List::stream).toList(), true);
    }
  }

  private boolean checkMidsThreeGeneric(DigitalSpecimen attributes) {
    return hasValue(attributes.getOdsOrganisationID()) && taxValuesComply(attributes)
        && geographicalValuesComply(attributes);
  }

  private boolean geographicalValuesComply(DigitalSpecimen attributes) {
    if (attributes.getOdsHasEvents() != null
        && attributes.getOdsHasEvents().get(0).getOdsHasLocation() != null) {
      var location = attributes.getOdsHasEvents().get(0).getOdsHasLocation();
      if (isValid(location.getDwcLocationID())) {
        return true;
      } else if (location.getOdsHasGeoreference() != null) {
        var geoReference = location.getOdsHasGeoreference();
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
    for (var identifications : attributes.getOdsHasIdentifications()) {
      if (hasAgentWithRole(IDENTIFIER, identifications.getOdsHasAgents(), true)) {
        hasIdentifiedById = true;
      }
      for (var taxonIdentification : identifications.getOdsHasTaxonIdentifications()) {
        if (isValid(taxonIdentification.getDwcScientificNameID())) {
          hasScientificNameId = true;
        }
      }
    }
    return hasScientificNameId && hasIdentifiedById;
  }

  private boolean compliesToMidsTwoBio(DigitalSpecimen attributes) {
    return Boolean.TRUE.equals(attributes.getOdsIsKnownToContainMedia())
        && qualitativeLocationIsValid(attributes)
        && quantitativeLocationIsValid(attributes)
        && (eventIsPresent(attributes)
        && hasValue(attributes.getOdsHasEvents().get(0).getDwcEventDate(),
        attributes.getOdsHasEvents().get(0).getDwcVerbatimEventDate(),
        convertInteger(attributes.getOdsHasEvents().get(0).getDwcYear())))
        && (eventIsPresent(attributes) && hasValue(
        attributes.getOdsHasEvents().get(0).getDwcFieldNumber())
        && hasAgentWithRole(COLLECTOR,
        attributes.getOdsHasEvents().stream().map(Event::getOdsHasAgents).flatMap(List::stream)
            .toList(), false));
  }

  private boolean hasAgentWithRole(AgentRoleType roleType, List<Agent> agents, boolean idRequired) {
    for (var agent : agents) {
      for (var role : agent.getOdsHasRoles()) {
        if (role.getSchemaRoleName().equals(roleType.getName())) {
          if (idRequired) {
            return isValid(agent.getId());
          } else {
            return true;
          }
        }
      }
    }
    return false;
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
        && digitalSpecimen.getOdsHasEvents().get(0).getOdsHasLocation().getOdsHasGeoreference()
        != null) {
      var location = digitalSpecimen.getOdsHasEvents().get(0).getOdsHasLocation();
      var geoReference = location.getOdsHasGeoreference();
      return hasValue(location.getDwcLocationID(), geoReference.getDwcFootprintWKT()) || (
          geoReference.getDwcDecimalLatitude() != null
              && geoReference.getDwcDecimalLongitude() != null);
    }
    return false;
  }

  private boolean eventIsPresent(DigitalSpecimen attributes) {
    return attributes.getOdsHasEvents() != null && !attributes.getOdsHasEvents().isEmpty()
        && attributes.getOdsHasEvents().get(0) != null;
  }

  private boolean locationIsPresent(DigitalSpecimen digitalSpecimen) {
    return eventIsPresent(digitalSpecimen)
        && digitalSpecimen.getOdsHasEvents().get(0).getOdsHasLocation() != null;
  }

  private boolean qualitativeLocationIsValid(DigitalSpecimen digitalSpecimen) {
    if (locationIsPresent(digitalSpecimen)) {
      var isValidValue = false;
      var location = digitalSpecimen.getOdsHasEvents().get(0).getOdsHasLocation();
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
      if (!isValidValue && (location.getOdsHasGeoreference() != null)) {
        isValidValue = isValid(location.getOdsHasGeoreference().getDwcVerbatimCoordinates());
        if (!isValidValue) {
          isValidValue = (isValid(location.getOdsHasGeoreference().getDwcVerbatimLatitude())
              && isValid(
              location.getOdsHasGeoreference().getDwcVerbatimLongitude()));
        }
      }
      return isValidValue;
    }
    return false;
  }

  private boolean stratigraphyIsValid(DigitalSpecimen digitalSpecimen) {
    if (locationIsPresent(digitalSpecimen)
        && digitalSpecimen.getOdsHasEvents().get(0).getOdsHasLocation().getOdsHasGeologicalContext()
        != null) {
      var geologicalContext = digitalSpecimen.getOdsHasEvents().get(0).getOdsHasLocation()
          .getOdsHasGeologicalContext();
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
