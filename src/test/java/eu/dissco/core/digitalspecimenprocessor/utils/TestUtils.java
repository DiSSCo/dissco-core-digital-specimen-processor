package eu.dissco.core.digitalspecimenprocessor.utils;

import static eu.dissco.core.digitalspecimenprocessor.domain.AgentRoleType.PROCESSING_SERVICE;
import static eu.dissco.core.digitalspecimenprocessor.domain.AgentRoleType.SOURCE_SYSTEM;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.MEDIA_HOST;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.MEDIA_HOST_NAME;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.MEDIA_TYPE;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.MIME_TYPE;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.PRIMARY_MEDIA_ID;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.PRIMARY_MEDIA_ID_NAME;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.PRIMARY_MEDIA_ID_TYPE;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.REFERENT_NAME;
import static eu.dissco.core.digitalspecimenprocessor.schema.Agent.Type.SCHEMA_SOFTWARE_APPLICATION;
import static eu.dissco.core.digitalspecimenprocessor.schema.Identifier.DctermsType.DOI;
import static eu.dissco.core.digitalspecimenprocessor.util.AgentUtils.createMachineAgent;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import eu.dissco.core.digitalspecimenprocessor.database.jooq.enums.MjrTargetType;
import eu.dissco.core.digitalspecimenprocessor.domain.AutoAcceptedAnnotation;
import eu.dissco.core.digitalspecimenprocessor.domain.EntityRelationshipType;
import eu.dissco.core.digitalspecimenprocessor.domain.FdoType;
import eu.dissco.core.digitalspecimenprocessor.domain.mas.MasJobRequest;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaEvent;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaRecord;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaWrapper;
import eu.dissco.core.digitalspecimenprocessor.domain.media.UpdatedDigitalMediaRecord;
import eu.dissco.core.digitalspecimenprocessor.domain.media.UpdatedDigitalMediaTuple;
import eu.dissco.core.digitalspecimenprocessor.domain.relation.DigitalMediaRelationshipTombstoneEvent;
import eu.dissco.core.digitalspecimenprocessor.domain.relation.MediaRelationshipProcessResult;
import eu.dissco.core.digitalspecimenprocessor.domain.relation.PidProcessResult;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenEvent;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenRecord;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenWrapper;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.UpdatedDigitalSpecimenRecord;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.UpdatedDigitalSpecimenTuple;
import eu.dissco.core.digitalspecimenprocessor.schema.AnnotationBody;
import eu.dissco.core.digitalspecimenprocessor.schema.AnnotationProcessingRequest;
import eu.dissco.core.digitalspecimenprocessor.schema.AnnotationProcessingRequest.OaMotivation;
import eu.dissco.core.digitalspecimenprocessor.schema.AnnotationTarget;
import eu.dissco.core.digitalspecimenprocessor.schema.DigitalMedia;
import eu.dissco.core.digitalspecimenprocessor.schema.DigitalSpecimen;
import eu.dissco.core.digitalspecimenprocessor.schema.DigitalSpecimen.OdsLivingOrPreserved;
import eu.dissco.core.digitalspecimenprocessor.schema.DigitalSpecimen.OdsPhysicalSpecimenIDType;
import eu.dissco.core.digitalspecimenprocessor.schema.DigitalSpecimen.OdsTopicDiscipline;
import eu.dissco.core.digitalspecimenprocessor.schema.EntityRelationship;
import eu.dissco.core.digitalspecimenprocessor.schema.Identifier;
import eu.dissco.core.digitalspecimenprocessor.schema.Identifier.DctermsType;
import eu.dissco.core.digitalspecimenprocessor.schema.OaHasSelector;
import eu.dissco.core.digitalspecimenprocessor.util.DigitalObjectUtils;
import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TestUtils {

  public static final ObjectMapper MAPPER = new ObjectMapper().findAndRegisterModules();
  public static final String HANDLE = "20.5000.1025/V1Z-176-LL4";
  public static final String DOI_PREFIX = "https://doi.org/";
  public static final String HANDLE_PREFIX = "https://hdl.handle.net/";
  public static final String SECOND_HANDLE = "20.5000.1025/XXX-XXX-XXX";
  public static final String THIRD_HANDLE = "20.5000.1025/YYY-YYY-YYY";
  public static final String APP_HANDLE = "https://doi.org/10.5281/zenodo.14383054";
  public static final String APP_NAME = "DiSSCo Digital Specimen Processing Service";
  public static final int MIDS_LEVEL = 1;
  public static final int VERSION = 1;
  public static final Instant CREATED = Instant.parse("2022-11-01T09:59:24.000Z");
  public static final String UPDATED_STR = "2023-11-01T09:59:24.000Z";
  public static final Date UPDATED = Date.from(Instant.parse(UPDATED_STR));
  public static final String MAS = "https://hdl.handle.net/20.5000.1025/TG2-A9R-ZDD";
  public static final String TYPE = "https://doi.org/21.T11148/894b1e6cad57e921764e";
  public static final String TYPE_MEDIA = "https://doi.org/21.T11148/bbad8c4e101e8af01115";
  public static final String PHYSICAL_SPECIMEN_ID = "https://geocollections.info/specimen/23602";
  public static final String PHYSICAL_SPECIMEN_ID_ALT = "A second specimen";
  public static final OdsPhysicalSpecimenIDType PHYSICAL_SPECIMEN_TYPE = OdsPhysicalSpecimenIDType.GLOBAL;
  public static final String SPECIMEN_NAME = "Biota";
  public static final String ANOTHER_SPECIMEN_NAME = "Another SpecimenName";
  public static final String ORGANISATION_ID = "https://ror.org/0443cwa12";
  public static final String ANOTHER_ORGANISATION = "Another organisation";
  public static final String DATASET_ID = null;
  public static final String PHYSICAL_SPECIMEN_COLLECTION = null;
  public static final String SOURCE_SYSTEM_ID = "https://hdl.handle.net/TEST/57Z-6PC-64W";
  public static final String SOURCE_SYSTEM_NAME = "A very nice source system";
  public static final JsonNode ORIGINAL_DATA = generateSpecimenOriginalData();
  public static final OdsTopicDiscipline TOPIC_DISCIPLINE = OdsTopicDiscipline.BOTANY;
  public static final String MEDIA_URL = "https://an-image.org";
  public static final String MEDIA_URL_ALT = MEDIA_URL + "/2";
  public static final String MEDIA_PID = "20.5000.1025/ZZZ-ZZZ-ZZZ";
  public static final String MEDIA_PID_ALT = "20.5000.1025/AAA-AAA-AAA";
  public static final String MEDIA_MAS = "https://hdl.handle.net/20.5000.1025/5E3-P4R-AQC";


  public static final String SPECIMEN_BASE_URL = "https://doi.org/";

  public static JsonNode generateSpecimenOriginalData() {
    try {
      return MAPPER.readTree(
          """
              {"abcd:unitID": "152-4972",
                "abcd:sourceID": "GIT",
                "abcd:unitGUID": "https://geocollections.info/specimen/23646",
                "abcd:recordURI": "https://geocollections.info/specimen/23646",
                "abcd:recordBasis": "FossilSpecimen",
                "abcd:unitIDNumeric": 23646,
                "abcd:dateLastEdited": "2004-06-09T10:17:54.000+00:00",
                "abcd:kindOfUnit/0/value": "",
                "abcd:sourceInstitutionID": "Department of Geology, TalTech",
                "abcd:kindOfUnit/0/language": "en",
                "abcd:gathering/country/name/value": "Estonia",
                "abcd:gathering/localityText/value": "Laeva 297 borehole",
                "abcd:gathering/country/iso3166Code": "EE",
                "abcd:gathering/localityText/language": "en",
                "abcd:gathering/altitude/measurementOrFactText/value": "39.9",
                "abcd:identifications/identification/0/preferredFlag": true,
                "abcd:gathering/depth/measurementOrFactAtomised/lowerValue/value": "165",
                "abcd:gathering/depth/measurementOrFactAtomised/unitOfMeasurement": "m",
                "abcd:gathering/siteCoordinateSets/siteCoordinates/0/coordinatesLatLong/spatialDatum": "WGS84",
                "abcd:gathering/stratigraphy/chronostratigraphicTerms/chronostratigraphicTerm/0/term": "Pirgu Stage",
                "abcd:gathering/stratigraphy/chronostratigraphicTerms/chronostratigraphicTerm/1/term": "Katian",
                "abcd:gathering/siteCoordinateSets/siteCoordinates/0/coordinatesLatLong/latitudeDecimal": 58.489269,
                "abcd:gathering/siteCoordinateSets/siteCoordinates/0/coordinatesLatLong/longitudeDecimal": 26.385719,
                "abcd:gathering/stratigraphy/chronostratigraphicTerms/chronostratigraphicTerm/0/language": "en",
                "abcd:gathering/stratigraphy/chronostratigraphicTerms/chronostratigraphicTerm/1/language": "en",
                "abcd:identifications/identification/0/result/taxonIdentified/scientificName/fullScientificNameString": "Biota",
                "abcd-efg:earthScienceSpecimen/unitStratigraphicDetermination/chronostratigraphicAttributions/chronostratigraphicAttribution/0/chronostratigraphicName": "Pirgu Stage",
                "abcd-efg:earthScienceSpecimen/unitStratigraphicDetermination/chronostratigraphicAttributions/chronostratigraphicAttribution/0/chronoStratigraphicDivision": "Stage"
              }"""
      );
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  public static DigitalSpecimenRecord givenDigitalSpecimenRecord(String handle) {
    return new DigitalSpecimenRecord(
        handle,
        MIDS_LEVEL,
        1,
        CREATED,
        givenDigitalSpecimenWrapper(),
        Set.of(MAS),
        false,
        true,
        List.of());
  }

  public static DigitalSpecimenRecord givenDigitalSpecimenRecord(int version, boolean hasMedia) {
    return new DigitalSpecimenRecord(
        HANDLE,
        MIDS_LEVEL,
        version,
        CREATED,
        givenDigitalSpecimenWrapper(hasMedia),
        Set.of(MAS),
        false,
        true,
        List.of());
  }

  public static DigitalSpecimenRecord givenDigitalSpecimenRecordWithMediaEr() {
    return givenDigitalSpecimenRecordWithMediaEr(HANDLE, PHYSICAL_SPECIMEN_ID, false);
  }

  public static DigitalSpecimenRecord givenDigitalSpecimenRecordWithMediaEr(String handle,
      String physicalId, boolean addOtherEntityRelationship, int version) {
    return givenDigitalSpecimenRecordWithMediaEr(handle, physicalId, addOtherEntityRelationship,
        version, MEDIA_PID);
  }

  public static DigitalSpecimenRecord givenDigitalSpecimenRecordWithMediaEr(String handle,
      String physicalId, boolean addOtherEntityRelationship, int version, String mediaId) {
    return new DigitalSpecimenRecord(
        handle,
        MIDS_LEVEL,
        version,
        CREATED,
        givenDigitalSpecimenWrapperWithMediaEr(physicalId, addOtherEntityRelationship, mediaId),
        Set.of(MAS), false, true, List.of(givenDigitalMediaEvent()));
  }

  public static DigitalSpecimenRecord givenDigitalSpecimenRecordWithMediaEr(String handle,
      String physicalId, boolean addOtherEntityRelationship) {
    return givenDigitalSpecimenRecordWithMediaEr(handle, physicalId, addOtherEntityRelationship, 1);
  }

  public static DigitalSpecimenRecord givenDigitalSpecimenRecord() {
    return givenDigitalSpecimenRecord(VERSION, false);
  }

  public static DigitalSpecimenRecord givenUnequalDigitalSpecimenRecord() {
    return givenUnequalDigitalSpecimenRecord(HANDLE, ANOTHER_SPECIMEN_NAME, ORGANISATION_ID, false
    );
  }

  public static DigitalSpecimenRecord givenUnequalDigitalSpecimenRecord(String organisation) {
    return givenUnequalDigitalSpecimenRecord(HANDLE, ANOTHER_SPECIMEN_NAME, organisation, false
    );
  }

  public static DigitalSpecimenRecord givenUnequalDigitalSpecimenRecord(String handle,
      String specimenName, String organisation, boolean hasMedia) {
    return givenUnequalDigitalSpecimenRecord(handle, specimenName, organisation, hasMedia,
        PHYSICAL_SPECIMEN_ID);
  }

  public static DigitalSpecimenRecord givenUnequalDigitalSpecimenRecord(String handle,
      String specimenName, String organisation, boolean hasMedia, String physicalSpecimenId) {
    return new DigitalSpecimenRecord(
        handle,
        MIDS_LEVEL,
        VERSION,
        CREATED,
        givenDigitalSpecimenWrapper(physicalSpecimenId, specimenName, organisation, false,
            hasMedia),
        Set.of(MAS), false, true,
        List.of());
  }

  public static DigitalSpecimenRecord givenDigitalSpecimenRecord(String handle,
      String physicalSpecimenId, boolean hasMedia) {
    List<DigitalMediaEvent> mediaEvents = hasMedia ? List.of(givenDigitalMediaEvent()) : List.of();
    return new DigitalSpecimenRecord(
        handle,
        MIDS_LEVEL,
        VERSION,
        CREATED,
        givenDigitalSpecimenWrapper(physicalSpecimenId, SPECIMEN_NAME, ORGANISATION_ID, false,
            hasMedia), Set.of(MAS), false, true,
        mediaEvents);
  }

  public static DigitalSpecimenEvent givenDigitalSpecimenEvent(String physicalSpecimenId) {
    return givenDigitalSpecimenEvent(physicalSpecimenId, true, true);
  }

  public static DigitalSpecimenEvent givenDigitalSpecimenEvent(String physicalSpecimenId,
      boolean hasMedia, boolean isDataFromSourceSystem) {
    List<DigitalMediaEvent> mediaEvents = hasMedia ? List.of(givenDigitalMediaEvent()) : List.of();
    return new DigitalSpecimenEvent(
        Set.of(MAS),
        givenDigitalSpecimenWrapper(physicalSpecimenId, SPECIMEN_NAME, ORGANISATION_ID, false,
            hasMedia),
        mediaEvents,
        false,
        isDataFromSourceSystem);
  }

  public static MediaRelationshipProcessResult givenEmptyMediaProcessResult() {
    return new MediaRelationshipProcessResult();
  }

  public static DigitalMediaRecord givenDigitalMediaRecord() {
    return givenDigitalMediaRecord(VERSION);
  }

  public static DigitalMediaRecord givenDigitalMediaRecord(int version) {
    return givenDigitalMediaRecord(MEDIA_PID, MEDIA_URL, version);
  }

  public static DigitalMediaRecord givenDigitalMediaRecord(String pid, String uri, int version) {
    return new DigitalMediaRecord(
        pid, uri, version, CREATED, Set.of(MEDIA_MAS),
        givenDigitalMedia(uri, true),
        MAPPER.createObjectNode(), false);
  }

  public static DigitalMediaWrapper givenDigitalMediaWrapper() {
    return givenDigitalMediaWrapper(MEDIA_URL, false);
  }

  public static DigitalMediaWrapper givenDigitalMediaWrapper(String url, boolean addSpecimenEr) {
    return new DigitalMediaWrapper(
        FdoType.MEDIA.getPid(),
        givenDigitalMedia(url, addSpecimenEr),
        MAPPER.createObjectNode()
    );
  }

  public static DigitalMedia givenDigitalMedia(String uri, boolean addSpecimenEr) {
    var ers = addSpecimenEr ? List.of(givenEntityRelationship(),
        givenEntityRelationship(HANDLE, EntityRelationshipType.HAS_SPECIMEN.getRelationshipName()))
        : List.of(givenEntityRelationship());
    return new DigitalMedia()
        .withOdsFdoType(FdoType.MEDIA.getPid())
        .withAcAccessURI(uri)
        .withOdsOrganisationID(ORGANISATION_ID)
        .withOdsHasEntityRelationships(ers);
  }

  public static DigitalMediaRecord givenDigitalMediaRecordNoMas() {
    return new DigitalMediaRecord(
        MEDIA_PID, MEDIA_URL, VERSION, CREATED, Set.of(),
        new DigitalMedia()
            .withOdsFdoType(FdoType.MEDIA.getPid())
            .withAcAccessURI(MEDIA_URL)
            .withOdsOrganisationID(ORGANISATION_ID)
            .withOdsHasEntityRelationships(
                List.of(givenEntityRelationship(), givenEntityRelationship(HANDLE,
                    EntityRelationshipType.HAS_SPECIMEN.getRelationshipName()))),
        MAPPER.createObjectNode(), null);
  }

  public static DigitalMedia givenDigitalMediaWithRelationship() {
    return new DigitalMedia()
        .withOdsFdoType(FdoType.MEDIA.getPid())
        .withAcAccessURI(MEDIA_URL)
        .withOdsOrganisationID(ORGANISATION_ID)
        .withOdsHasEntityRelationships(
            List.of(givenEntityRelationship(), givenEntityRelationship(HANDLE,
                EntityRelationshipType.HAS_SPECIMEN.getRelationshipName())));
  }


  public static DigitalMediaEvent givenUnequalDigitalMediaEvent() {
    return givenUnequalDigitalMediaEvent(MEDIA_URL_ALT, false);
  }

  public static DigitalMediaEvent givenUnequalDigitalMediaEvent(String url, boolean addSpecimenEr) {
    return new DigitalMediaEvent(Set.of(MEDIA_MAS),
        new DigitalMediaWrapper(
            FdoType.MEDIA.getPid(),
            givenUnequalDigitalMedia(url, addSpecimenEr),
            MAPPER.createObjectNode()),
        false);
  }

  public static DigitalMediaRecord givenUnequalDigitalMediaRecord() {
    return givenUnequalDigitalMediaRecord(MEDIA_PID, MEDIA_URL, VERSION);
  }

  public static DigitalMediaRecord givenUnequalDigitalMediaRecord(int version) {
    return givenUnequalDigitalMediaRecord(MEDIA_PID, MEDIA_URL, version);
  }

  public static DigitalMediaRecord givenUnequalDigitalMediaRecord(String pid, String url,
      int version) {
    var media = givenUnequalDigitalMedia(url, true);
    return new DigitalMediaRecord(
        pid, url, version, CREATED, Set.of(MEDIA_MAS), media, MAPPER.createObjectNode(), false);
  }

  public static DigitalMedia givenUnequalDigitalMedia(String url, boolean addSpecimenEr) {
    List<EntityRelationship> ers =
        addSpecimenEr ? List.of(givenEntityRelationship(), givenEntityRelationship(HANDLE,
            EntityRelationshipType.HAS_SPECIMEN.getRelationshipName()))
            : List.of(givenEntityRelationship());
    return new DigitalMedia()
        .withOdsFdoType(FdoType.MEDIA.getPid())
        .withAcAccessURI(url)
        .withOdsOrganisationName(ANOTHER_ORGANISATION)
        .withOdsHasEntityRelationships(ers);
  }

  public static DigitalSpecimenEvent givenDigitalSpecimenEvent() {
    return givenDigitalSpecimenEvent(false);
  }

  public static DigitalSpecimenEvent givenDigitalSpecimenEvent(boolean hasMedia,
      boolean entityRelationship, boolean isDataFromSourceSystem) {
    return new DigitalSpecimenEvent(
        Set.of(MAS),
        givenDigitalSpecimenWrapper(entityRelationship, hasMedia),
        hasMedia ? List.of(givenDigitalMediaEvent()) : List.of(),
        false,
        isDataFromSourceSystem);
  }

  public static DigitalSpecimenWrapper givenDigitalSpecimenWrapperWithMediaEr(String physicalId,
      Boolean addOtherEntityRelationship) {
    return givenDigitalSpecimenWrapperWithMediaEr(physicalId, addOtherEntityRelationship,
        MEDIA_PID);
  }

  public static DigitalSpecimenWrapper givenDigitalSpecimenWrapperWithMediaEr(String physicalId,
      Boolean addOtherEntityRelationship, String mediaId) {
    var attributes = givenAttributes(SPECIMEN_NAME, ORGANISATION_ID, true,
        addOtherEntityRelationship, true);
    var entityRelationships = new ArrayList<>(attributes.getOdsHasEntityRelationships());
    entityRelationships.add(
        givenEntityRelationship(mediaId, EntityRelationshipType.HAS_MEDIA.getRelationshipName())
    );
    attributes.setOdsHasEntityRelationships(entityRelationships);
    return new DigitalSpecimenWrapper(physicalId, TYPE, attributes,
        ORIGINAL_DATA);
  }

  public static EntityRelationship givenEntityRelationship(String relatedId,
      String relationshipType) {
    return new EntityRelationship()
        .withType("ods:EntityRelationship")
        .withDwcRelationshipEstablishedDate(Date.from(CREATED))
        .withDwcRelationshipOfResource(relationshipType)
        .withOdsHasAgents(List.of(createMachineAgent(APP_NAME, APP_HANDLE, PROCESSING_SERVICE,
            DOI, SCHEMA_SOFTWARE_APPLICATION)))
        .withDwcRelatedResourceID(DOI_PREFIX + relatedId)
        .withOdsRelatedResourceURI(URI.create(DOI_PREFIX + relatedId));
  }

  public static EntityRelationship givenEntityRelationship() {
    return givenEntityRelationship(HANDLE, "hasSomeRelationship");
  }

  public static DigitalSpecimenEvent givenDigitalSpecimenEvent(boolean hasMedia) {
    return givenDigitalSpecimenEvent(hasMedia, false, true);
  }

  public static DigitalMediaEvent givenDigitalMediaEventWithSpecimenEr() {
    return new DigitalMediaEvent(
        Set.of(MEDIA_MAS),
        givenDigitalMediaWrapper(MEDIA_URL, true), false);

  }

  public static DigitalMediaEvent givenDigitalMediaEvent(String mediaUrl) {
    return new DigitalMediaEvent(
        Set.of(MEDIA_MAS),
        givenDigitalMediaWrapper(mediaUrl, false), false);
  }

  public static DigitalMediaEvent givenDigitalMediaEvent() {
    return givenDigitalMediaEvent(MEDIA_URL);
  }

  public static DigitalMediaEvent givenDigitalMediaEventWithRelationship() {
    return givenDigitalMediaEventWithRelationship(null);
  }

  public static DigitalMediaEvent givenDigitalMediaEventWithRelationship(String id) {
    var digitalMedia = new DigitalMedia()
        .withOdsFdoType(FdoType.MEDIA.getPid())
        .withAcAccessURI(MEDIA_URL)
        .withId(id)
        .withDctermsIdentifier(id)
        .withOdsOrganisationID(ORGANISATION_ID)
        .withOdsHasEntityRelationships(
            List.of(new EntityRelationship()
                .withType("ods:EntityRelationship")
                .withDwcRelationshipOfResource("hasDigitalSpecimen")
                .withDwcRelationshipEstablishedDate(Date.from(CREATED))
                .withDwcRelatedResourceID(DOI_PREFIX + HANDLE)
                .withOdsRelatedResourceURI(URI.create(DOI_PREFIX + HANDLE))
                .withOdsHasAgents(List.of(createMachineAgent(APP_NAME, APP_HANDLE,
                    PROCESSING_SERVICE, DOI, SCHEMA_SOFTWARE_APPLICATION)))));
    return new DigitalMediaEvent(
        Set.of(MEDIA_MAS),
        new DigitalMediaWrapper(
            "StillImage",
            digitalMedia,
            MAPPER.createObjectNode()
        ), false);
  }

  public static DigitalSpecimenWrapper givenDigitalSpecimenWrapper() {
    return givenDigitalSpecimenWrapper(false, false);
  }

  public static DigitalSpecimenWrapper givenDigitalSpecimenWrapper(boolean hasMedia) {
    return givenDigitalSpecimenWrapper(false, hasMedia);
  }

  public static DigitalSpecimenWrapper givenDigitalSpecimenWrapper(boolean entityRelationship,
      boolean hasMedia) {
    return givenDigitalSpecimenWrapper(PHYSICAL_SPECIMEN_ID, SPECIMEN_NAME, ORGANISATION_ID,
        entityRelationship, hasMedia);
  }

  public static DigitalSpecimenWrapper givenDigitalSpecimenWrapper(String physicalSpecimenId,
      String specimenName, String organisation, boolean entityRelationship, boolean hasMedia) {
    return new DigitalSpecimenWrapper(
        physicalSpecimenId,
        TYPE,
        givenAttributes(specimenName, organisation, true, entityRelationship, hasMedia),
        ORIGINAL_DATA
    );
  }

  public static DigitalSpecimenWrapper givenDigitalSpecimenWrapperNoOriginalData() {
    return new DigitalSpecimenWrapper(
        PHYSICAL_SPECIMEN_ID,
        TYPE,
        givenAttributes(SPECIMEN_NAME, ORGANISATION_ID, true, false, false),
        MAPPER.createObjectNode()
    );
  }

  public static Map<String, String> givenHandleResponseSpecimen() {
    return givenHandleResponse(List.of(PHYSICAL_SPECIMEN_ID), List.of(HANDLE));
  }

  public static Map<String, String> givenHandleResponseMedia() {
    return givenHandleResponse(List.of(MEDIA_URL), List.of(MEDIA_PID));
  }

  public static Map<String, String> givenHandleResponse(List<String> localIds,
      List<String> handles) {
    assert (localIds.size() == handles.size());
    Map<String, String> pidMap = new HashMap<>();
    for (int i = 0; i < localIds.size(); i++) {
      pidMap.put(localIds.get(i), handles.get(i));
    }
    return pidMap;
  }

  public static Map<String, String> givenHandleResponseSpecimen(
      List<DigitalSpecimenRecord> records) {
    Map<String, String> response = new HashMap<>();
    for (var specimenRecord : records) {
      response.put(specimenRecord.digitalSpecimenWrapper().physicalSpecimenID(),
          specimenRecord.id());
    }
    return response;
  }

  public static JsonNode givenUpdateHandleRequest() throws Exception {
    return givenUpdateHandleRequest(null);
  }

  public static JsonNode givenUpdateHandleRequest(Boolean markedAsType) throws Exception {
    var attributes = givenHandleAttributes(markedAsType);
    return MAPPER.createObjectNode()
        .set("data", MAPPER.createObjectNode()
            .put("id", HANDLE)
            .put("type", TYPE)
            .set("attributes", attributes));
  }

  public static JsonNode givenHandleMediaRequest() {
    return MAPPER.createObjectNode()
        .set("data", MAPPER.createObjectNode()
            .put("type", TYPE_MEDIA)
            .set("attributes", givenHandleMediaRequestAttributes()));
  }

  public static JsonNode givenHandleMediaRequestAttributes() {
    return MAPPER.createObjectNode()
        .put(REFERENT_NAME.getAttribute(), MEDIA_URL)
        .put(MEDIA_HOST.getAttribute(), ORGANISATION_ID)
        .put(MEDIA_HOST_NAME.getAttribute(), (String) null)
        .put(PRIMARY_MEDIA_ID.getAttribute(), MEDIA_URL)
        .put(PRIMARY_MEDIA_ID_TYPE.getAttribute(), "Resolvable")
        .put(PRIMARY_MEDIA_ID_NAME.getAttribute(), "ac:accessURI")
        .put(MEDIA_TYPE.getAttribute(), "image")
        .put(MIME_TYPE.getAttribute(), (String) null);
  }


  public static JsonNode givenHandleRequest() throws Exception {
    return givenHandleRequest(null);
  }

  public static JsonNode givenHandleRequest(Boolean markedAsType) throws Exception {
    var attributes = (ObjectNode) givenHandleAttributes(markedAsType);
    return MAPPER.createObjectNode()
        .set("data", MAPPER.createObjectNode()
            .put("type", TYPE)
            .set("attributes", attributes));
  }

  public static JsonNode givenHandleAttributes(Boolean markedAsType) throws Exception {
    var attributes = (ObjectNode) MAPPER.readTree("""
        {
          "normalisedPrimarySpecimenObjectId":"https://geocollections.info/specimen/23602",
          "specimenHost": "https://ror.org/0443cwa12",
          "specimenHostName": "National Museum of Natural History",
          "topicDiscipline": "Botany",
          "referentName": "Biota",
          "livingOrPreserved": "Preserved",
          "otherSpecimenIds":[{"identifierType":"physical specimen identifier","identifierValue": "https://geocollections.info/specimen/23602","resolvable":false}]
        }
        """);
    if (markedAsType != null) {
      attributes.put("markedAsType", markedAsType);
    }
    return attributes;
  }

  public static JsonNode givenHandleRequestMin() throws Exception {
    return MAPPER.readTree("""
        {
          "data": {
            "type": "https://doi.org/21.T11148/894b1e6cad57e921764e",
            "attributes": {
              "normalisedPrimarySpecimenObjectId": "https://geocollections.info/specimen/23602",
              "specimenHost": "https://ror.org/0443cwa12",
              "referentName": "Specimen from https://ror.org/0443cwa12",
              "otherSpecimenIds":[{"identifierType":"physical specimen identifier","identifierValue": "https://geocollections.info/specimen/23602","resolvable":false}]
            }
          }
        }
        """);
  }

  public static UpdatedDigitalSpecimenRecord givenUpdatedDigitalSpecimenRecord(boolean hasMedia)
      throws JsonProcessingException {
    return givenUpdatedDigitalSpecimenRecord(givenUnequalDigitalSpecimenRecord(), hasMedia);
  }

  public static UpdatedDigitalSpecimenRecord givenUpdatedDigitalSpecimenRecord(
      DigitalSpecimenRecord currentRecord, boolean hasMedia) throws JsonProcessingException {
    if (hasMedia) {
      return new UpdatedDigitalSpecimenRecord(
          givenDigitalSpecimenRecord(2, true),
          Set.of(MAS),
          currentRecord,
          givenJsonPatchSpecimen(),
          List.of(givenDigitalMediaEvent()),
          new MediaRelationshipProcessResult(
              List.of(),
              List.of(givenDigitalMediaEvent()),
              List.of()
          ), true);
    }
    return new UpdatedDigitalSpecimenRecord(
        givenDigitalSpecimenRecord(2, false),
        Set.of(MAS),
        currentRecord,
        givenJsonPatchSpecimen(),
        List.of(),
        givenEmptyMediaProcessResult(),
        true);
  }

  public static DigitalSpecimen givenAttributes(
      String specimenName, String organisation, Boolean markedAsType,
      boolean addEntityRelationShip, boolean containsMedia) {
    var ds = new DigitalSpecimen()
        .withOdsOrganisationID(organisation)
        .withOdsOrganisationName("National Museum of Natural History")
        .withOdsPhysicalSpecimenIDType(PHYSICAL_SPECIMEN_TYPE)
        .withOdsPhysicalSpecimenID(PHYSICAL_SPECIMEN_ID)
        .withOdsNormalisedPhysicalSpecimenID(PHYSICAL_SPECIMEN_ID)
        .withOdsSpecimenName(specimenName)
        .withOdsTopicDiscipline(TOPIC_DISCIPLINE)
        .withOdsSourceSystemID(SOURCE_SYSTEM_ID)
        .withOdsSourceSystemName(SOURCE_SYSTEM_NAME)
        .withOdsLivingOrPreserved(OdsLivingOrPreserved.PRESERVED)
        .withDctermsLicense("http://creativecommons.org/licenses/by-nc/4.0/")
        .withDwcCollectionID(PHYSICAL_SPECIMEN_COLLECTION)
        .withDwcDatasetName(DATASET_ID)
        .withOdsIsMarkedAsType(markedAsType)
        .withOdsIsKnownToContainMedia(containsMedia)
        .withDctermsModified("2022-11-01T09:59:24.000Z");
    if (addEntityRelationShip) {
      ds.withOdsHasEntityRelationships(
          List.of(givenEntityRelationship()));
    }
    return ds;
  }

  public static DigitalSpecimen givenAttributesPlusIdentifier(String specimenName,
      String organisation, Boolean markedAsType) {
    return givenAttributes(specimenName, organisation, markedAsType, false, false)
        .withOdsHasIdentifiers(List.of(
            new Identifier().withDctermsTitle("Specimen label").withDctermsIdentifier(HANDLE)));
  }

  public static AutoAcceptedAnnotation givenAutoAcceptedAnnotation(
      AnnotationProcessingRequest annotation) {
    return new AutoAcceptedAnnotation(
        createMachineAgent(APP_NAME, APP_HANDLE, PROCESSING_SERVICE, DOI,
            SCHEMA_SOFTWARE_APPLICATION), annotation);
  }

  public static AnnotationProcessingRequest givenNewAcceptedAnnotation()
      throws JsonProcessingException {
    return new AnnotationProcessingRequest()
        .withOaMotivation(OaMotivation.ODS_ADDING)
        .withOaMotivatedBy("New information received from Source System with id: "
            + SOURCE_SYSTEM_ID)
        .withOaHasBody(new AnnotationBody()
            .withType("oa:TextualBody")
            .withOaValue(List.of(MAPPER.writeValueAsString(
                DigitalObjectUtils.flattenToDigitalSpecimen(givenDigitalSpecimenRecord()))))
            .withDctermsReferences(SOURCE_SYSTEM_ID))
        .withDctermsCreated(Date.from(CREATED))
        .withDctermsCreator(createMachineAgent(SOURCE_SYSTEM_NAME, SOURCE_SYSTEM_ID, SOURCE_SYSTEM,
            DctermsType.HANDLE, SCHEMA_SOFTWARE_APPLICATION))
        .withOaHasTarget(new AnnotationTarget()
            .withId(DOI_PREFIX + HANDLE)
            .withDctermsIdentifier(DOI_PREFIX + HANDLE)
            .withType(TYPE)
            .withOdsFdoType(TYPE)
            .withOaHasSelector(new OaHasSelector()
                .withAdditionalProperty("@type", "ods:ClassSelector")
                .withAdditionalProperty("ods:class", "$")));

  }

  public static JsonNode givenJsonPatchSpecimen() throws JsonProcessingException {
    return MAPPER.readTree(
        "[{\"op\":\"replace\",\"path\":\"/ods:specimenName\",\"value\":\"Biota\"}]");
  }

  public static JsonNode givenJsonPatchMedia() throws JsonProcessingException {
    return MAPPER.readTree("""
        [ {
          "op" : "remove",
          "path" : "/ods:organisationName"
        }, {
          "op" : "add",
          "path" : "/ods:organisationID",
          "value" : "https://ror.org/0443cwa12"
        } ]
        """);
  }

  public static PidProcessResult givenPidProcessResultSpecimen(boolean hasMedia) {
    var relatedDois = hasMedia ? Set.of(MEDIA_PID) : new HashSet<String>();
    return new PidProcessResult(HANDLE, relatedDois);
  }

  public static PidProcessResult givenPidProcessResultMedia() {
    return new PidProcessResult(MEDIA_PID, Set.of(HANDLE));
  }

  public static UpdatedDigitalSpecimenTuple givenUpdatedDigitalSpecimenTuple(boolean hasMedia,
      MediaRelationshipProcessResult mediaRelations) {
    return givenUpdatedDigitalSpecimenTuple(hasMedia, mediaRelations, true);
  }

  public static UpdatedDigitalSpecimenTuple givenUpdatedDigitalSpecimenTuple(boolean hasMedia,
      MediaRelationshipProcessResult mediaRelations, boolean isDataFromSourceSystem) {
    return new UpdatedDigitalSpecimenTuple(
        givenUnequalDigitalSpecimenRecord(HANDLE, ANOTHER_SPECIMEN_NAME, ORGANISATION_ID, hasMedia),
        givenDigitalSpecimenEvent(hasMedia, false, isDataFromSourceSystem),
        mediaRelations
    );
  }

  public static UpdatedDigitalMediaTuple givenUpdatedDigitalMediaTuple(boolean hasNewSpecimen) {
    var specimenRelations = hasNewSpecimen ? Set.of(HANDLE) : new HashSet<String>();
    return new UpdatedDigitalMediaTuple(
        givenUnequalDigitalMediaRecord(),
        givenDigitalMediaEvent(),
        specimenRelations
    );
  }

  public static UpdatedDigitalMediaRecord givenUpdatedDigitalMediaRecord() throws Exception {
    return new UpdatedDigitalMediaRecord(
        givenDigitalMediaRecord(2),
        Set.of(MEDIA_MAS),
        givenDigitalMediaRecord(),
        givenJsonPatchMedia()
    );
  }

  public static UpdatedDigitalMediaRecord givenUpdatedDigitalMediaRecord(String pid, String uri)
      throws Exception {
    return new UpdatedDigitalMediaRecord(
        givenDigitalMediaRecord(pid, uri, 2),
        Set
            .of(MEDIA_MAS),
        givenDigitalMediaRecord(pid, uri, 1),
        givenJsonPatchMedia()
    );
  }


  public static MasJobRequest givenMasJobRequestMedia() {
    return new MasJobRequest(
        MEDIA_MAS,
        MEDIA_PID,
        false,
        APP_HANDLE,
        MjrTargetType.MEDIA_OBJECT
    );
  }

  public static DigitalMediaRelationshipTombstoneEvent givenDigitalMediaTombstoneEvent() {
    return new DigitalMediaRelationshipTombstoneEvent(HANDLE, MEDIA_PID);
  }


}
