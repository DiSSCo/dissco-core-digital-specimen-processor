package eu.dissco.core.digitalspecimenprocessor.util;

import static eu.dissco.core.digitalspecimenprocessor.domain.AgentRoleType.PROCESSING_SERVICE;
import static eu.dissco.core.digitalspecimenprocessor.schema.Agent.Type.SCHEMA_SOFTWARE_APPLICATION;
import static eu.dissco.core.digitalspecimenprocessor.schema.Identifier.DctermsType.DOI;

import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaRecord;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenRecord;
import eu.dissco.core.digitalspecimenprocessor.property.ApplicationProperties;
import eu.dissco.core.digitalspecimenprocessor.schema.DigitalMedia;
import eu.dissco.core.digitalspecimenprocessor.schema.DigitalSpecimen;
import eu.dissco.core.digitalspecimenprocessor.schema.EntityRelationship;
import java.net.URI;
import java.time.Instant;
import java.util.Date;
import java.util.List;

public class DigitalObjectUtils {

  public static final String DOI_PROXY = "https://doi.org/";
  public static final String DLQ_FAILED = "Fatal exception, unable to dead letter queue: {}";
  public static final ApplicationProperties applicationProperties = new ApplicationProperties();

  private DigitalObjectUtils() {
  }

  public static String getIdforUri(URI uri) {
    return uri.toString().replace(DOI_PROXY, "");
  }

  public static DigitalSpecimen flattenToDigitalSpecimen(
      DigitalSpecimenRecord digitalSpecimenrecord) {
    var digitalSpecimen = digitalSpecimenrecord.digitalSpecimenWrapper().attributes();
    digitalSpecimen.setId(DOI_PROXY + digitalSpecimenrecord.id());
    digitalSpecimen.setDctermsIdentifier(DOI_PROXY + digitalSpecimenrecord.id());
    digitalSpecimen.setOdsVersion(digitalSpecimenrecord.version());
    digitalSpecimen.setOdsMidsLevel(digitalSpecimenrecord.midsLevel());
    digitalSpecimen.setDctermsCreated(Date.from(digitalSpecimenrecord.created()));
    return digitalSpecimen;
  }

  public static DigitalMedia flattenToDigitalMedia(DigitalMediaRecord digitalMediaRecord) {
    var digitalMedia = digitalMediaRecord.attributes();
    digitalMedia.setId(DOI_PROXY + digitalMediaRecord.id());
    digitalMedia.setDctermsIdentifier(DOI_PROXY + digitalMediaRecord.id());
    digitalMedia.setOdsVersion(digitalMediaRecord.version());
    digitalMedia.setDctermsCreated(Date.from(digitalMediaRecord.created()));
    return digitalMedia;
  }

  public static EntityRelationship buildEntityRelationship(String relationshipType,
      String relatedResourceId) {
    return new EntityRelationship()
        .withType("ods:EntityRelationship")
        .withDwcRelationshipEstablishedDate(Date.from(Instant.now()))
        .withDwcRelationshipOfResource(relationshipType)
        .withOdsHasAgents(List.of(AgentUtils.createMachineAgent(applicationProperties.getName(),
            applicationProperties.getPid(), PROCESSING_SERVICE, DOI, SCHEMA_SOFTWARE_APPLICATION)))
        .withDwcRelatedResourceID(DOI_PROXY + relatedResourceId)
        .withOdsRelatedResourceURI(URI.create(DOI_PROXY + relatedResourceId));
  }

}
