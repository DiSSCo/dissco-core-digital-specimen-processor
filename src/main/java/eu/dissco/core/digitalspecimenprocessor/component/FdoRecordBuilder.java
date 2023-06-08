package eu.dissco.core.digitalspecimenprocessor.component;

import static eu.dissco.core.digitalspecimenprocessor.database.jooq.Tables.HANDLES;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.DIGITAL_OBJECT_NAME;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.FDO_PROFILE;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.FDO_RECORD_LICENSE;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.HS_ADMIN;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.ISSUED_FOR_AGENT_NAME;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.LOC;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.MARKED_AS_TYPE;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.OBJECT_TYPE;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.PID;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.PID_ISSUER;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.PID_ISSUER_NAME;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.PID_STATUS;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.PRIMARY_REFERENT_TYPE;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.PRIMARY_SPECIMEN_OBJECT_ID;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.PRIMARY_SPECIMEN_OBJECT_ID_NAME;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.PRIMARY_SPECIMEN_OBJECT_ID_TYPE;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.REFERENT;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.REFERENT_DOI_NAME;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.REFERENT_NAME;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.REFERENT_TYPE;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.SPECIMEN_HOST;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.SPECIMEN_HOST_NAME;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.STRUCTURAL_TYPE;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.TOPIC_DISCIPLINE;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import eu.dissco.core.digitalspecimenprocessor.domain.DigitalSpecimen;
import eu.dissco.core.digitalspecimenprocessor.domain.DigitalSpecimenRecord;
import eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes;
import eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileConstants;
import eu.dissco.core.digitalspecimenprocessor.domain.HandleAttribute;
import eu.dissco.core.digitalspecimenprocessor.domain.UpdatedDigitalSpecimenTuple;
import eu.dissco.core.digitalspecimenprocessor.exception.PidCreationException;
import eu.dissco.core.digitalspecimenprocessor.repository.HandleRepository;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.w3c.dom.Document;

@Slf4j
@Component
@RequiredArgsConstructor
public class FdoRecordBuilder {

  private final ObjectMapper mapper;
  private static final String HANDLE_PROXY = "https://hdl.handle.net/";
  private static final byte[] FDO_RECORD_LICENSE_LOC = "https://creativecommons.org/publicdomain/zero/1.0/".getBytes(
      StandardCharsets.UTF_8);
  private static final Set<String> NOT_TYPE_STATUS = new HashSet<>(
      Arrays.asList("false", "specimen", "type"));

  private static final HashMap<String, String> odsMap;

  static {
    HashMap<String, String> map = new HashMap<>();
    map.put("ods:specimenName", FdoProfileAttributes.REFERENT_NAME.getAttribute());
    map.put("ods:organisationName", FdoProfileAttributes.SPECIMEN_HOST_NAME.getAttribute());
    map.put("ods:topicDiscipline", TOPIC_DISCIPLINE.getAttribute());
    map.put("ods:physicalSpecimenIdType",
        FdoProfileAttributes.PRIMARY_SPECIMEN_OBJECT_ID_TYPE.getAttribute());
    odsMap = map;
  }

  private static final String ODS_PREFIX = "ods:";
  private static final String PREFIX = "20.5000.1025/";
  private static final String TO_BE_FIXED = "Needs to be fixed!";
  private final char[] symbols = "ABCDEFGHJKLMNPQRSTVWXYZ1234567890".toCharArray();
  private final char[] buffer = new char[11];
  private final Random random;
  private final DocumentBuilder documentBuilder;
  private final HandleRepository repository;
  private final TransformerFactory transformerFactory;
  private final DateTimeFormatter dt = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
      .withZone(ZoneId.of("UTC"));

  public List<JsonNode> genCreateHandleRequest(List<DigitalSpecimen> digitalSpecimens)
      throws PidCreationException {
    List<JsonNode> requestBody = new ArrayList<>();
    for (var specimen : digitalSpecimens) {
      requestBody.add(genCreateHandleRequest(specimen));
    }
    return requestBody;
  }


  private JsonNode genCreateHandleRequest(DigitalSpecimen specimen) throws PidCreationException {
    var request = mapper.createObjectNode();
    var data = mapper.createObjectNode();
    data.put("type", FdoProfileConstants.DIGITAL_SPECIMEN_TYPE.getValue());
    var attributes = genRequestAttributes(specimen);
    data.set("attributes", attributes);
    request.set("data", data);
    return request;
  }

  private JsonNode genRequestAttributes(DigitalSpecimen specimen) throws PidCreationException {
    var attributes = mapper.createObjectNode();
    // Defaults
    attributes.put(FdoProfileAttributes.FDO_PROFILE.getAttribute(),
        FdoProfileConstants.FDO_PROFILE.getValue());
    attributes.put(FdoProfileAttributes.DIGITAL_OBJECT_TYPE.getAttribute(),
        FdoProfileConstants.DIGITAL_OBJECT_TYPE.getValue());
    attributes.put(FdoProfileAttributes.ISSUED_FOR_AGENT.getAttribute(),
        FdoProfileConstants.ISSUED_FOR_AGENT_PID.getValue());

    // Mandatory
    attributes.put(FdoProfileAttributes.PRIMARY_SPECIMEN_OBJECT_ID.getAttribute(),
        specimen.physicalSpecimenId());
    var organisationId = getTerm(specimen, "ods:organisationId");
    organisationId.ifPresent(orgId -> attributes.put(SPECIMEN_HOST.getAttribute(), orgId));
    if (organisationId.isEmpty()) {
      throw new PidCreationException(
          "Digital Specimen missing ods:organisationId. Unable to create PID. Check specimen"
              + specimen.physicalSpecimenId());
    }

    // Optional
    odsMap.forEach(
        (odsTerm, fdoAttribute) -> updateOptionalAttribute(specimen, odsTerm, fdoAttribute,
            attributes));

    //Must be lower case
    var livingOrPreserved = getTerm(specimen, "ods:livingOrPreserved");
    livingOrPreserved.ifPresent(
        foundTerm -> attributes.put(FdoProfileAttributes.LIVING_OR_PRESERVED.getAttribute(),
            foundTerm.toLowerCase()));

    setMarkedAsType(specimen, attributes);

    return attributes;
  }

  private void setMarkedAsType(DigitalSpecimen specimen, ObjectNode attributeNode) {
    // If typeStatus is present and NOT ["false", "specimen", "type"], this is to true, otherwise left blank.
    var markedAsType = getTerm(specimen, "dwc:typeStatus");
    if (markedAsType.isPresent() && !NOT_TYPE_STATUS.contains(markedAsType.get())) {
      attributeNode.put(FdoProfileAttributes.MARKED_AS_TYPE.getAttribute(), true);
    }
  }

  private void updateOptionalAttribute(DigitalSpecimen specimen, String term, String fdoAttribute,
      ObjectNode attributeNode) {
    var optionalAttribute = getTerm(specimen, term);
    optionalAttribute.ifPresent(foundTerm -> attributeNode.put(fdoAttribute, foundTerm));
  }

  private Optional<String> getTerm(DigitalSpecimen specimen, String term) {
    var val = specimen.attributes().get(term);
    return val == null ? Optional.empty() : Optional.of(val.asText());
  }

  //  The following functions will be depreciated in next PR
  public String createNewHandle(DigitalSpecimen digitalSpecimen)
      throws TransformerException, PidCreationException {
    var existingHandle = checkForPrimarySpecimenObjectId(digitalSpecimen);
    if (existingHandle.isPresent()) {
      log.info("Digital specimen with id {} already exists under handle {}. Updating FDO Record.",
          digitalSpecimen.physicalSpecimenId(), existingHandle.get());
      updateHandle(existingHandle.get(), digitalSpecimen);
      return existingHandle.get();
    }
    var handle = generateHandle();
    var recordTimestamp = Instant.now();
    var handleAttributes = fillFdoRecord(digitalSpecimen, handle, recordTimestamp);
    repository.createHandle(handle, recordTimestamp, handleAttributes);
    return handle;
  }

  private Optional<String> checkForPrimarySpecimenObjectId(DigitalSpecimen digitalSpecimen) {
    var primarySpecimenObjectId = digitalSpecimen.physicalSpecimenId()
        .getBytes(StandardCharsets.UTF_8);
    var optionalRecord = repository.searchByPrimarySpecimenObjectId(primarySpecimenObjectId);
    return optionalRecord.map(
        attribute -> new String(attribute.get(HANDLES.HANDLE), StandardCharsets.UTF_8));
  }

  private List<HandleAttribute> fillFdoRecord(DigitalSpecimen digitalSpecimen, String handle,
      Instant recordTimeStamp)
      throws TransformerException {
    List<HandleAttribute> fdoRecord = new ArrayList<>();
    fdoRecord.addAll(fillFdoRecordGeneratedAttributes(handle, recordTimeStamp));
    fdoRecord.addAll(fillFdoRecordSpecimenAttributes(digitalSpecimen));
    return fdoRecord;
  }

  // This will be depreciated next PR -> These attributes are addressed in the Handle API
  private List<HandleAttribute> fillFdoRecordGeneratedAttributes(String handle,
      Instant recordTimeStamp)
      throws TransformerException {
    List<HandleAttribute> fdoRecord = new ArrayList<>();

    // 100: Admin Handle
    fdoRecord.add(
        new HandleAttribute(HS_ADMIN.getIndex(), HS_ADMIN.getAttribute(), decodeAdmin()));

    // 101: 10320/loc
    byte[] loc = createLocations(handle);
    fdoRecord.add(new HandleAttribute(LOC.getIndex(), LOC.getAttribute(), loc));

    // 1: FDO Profile
    fdoRecord.add(
        new HandleAttribute(FDO_PROFILE.getIndex(), FDO_PROFILE.getAttribute(),
            FdoProfileConstants.FDO_PROFILE.getValue().getBytes(StandardCharsets.UTF_8)));

    // 2: FDO Record License
    fdoRecord.add(new HandleAttribute(FDO_RECORD_LICENSE.getIndex(),
        FDO_RECORD_LICENSE.getAttribute(), FDO_RECORD_LICENSE_LOC));

    // 3: DigitalObjectType
    fdoRecord.add(
        new HandleAttribute(FdoProfileAttributes.DIGITAL_OBJECT_TYPE.getIndex(),
            FdoProfileAttributes.DIGITAL_OBJECT_TYPE.getAttribute(),
            FdoProfileConstants.DIGITAL_OBJECT_TYPE.getValue().getBytes(StandardCharsets.UTF_8)));

    // 4: DigitalObjectName
    fdoRecord.add(
        new HandleAttribute(DIGITAL_OBJECT_NAME.getIndex(), DIGITAL_OBJECT_NAME.getAttribute(),
            "digitalSpecimen".getBytes(StandardCharsets.UTF_8)));

    // 5: Pid
    byte[] pid = (HANDLE_PROXY + handle).getBytes(StandardCharsets.UTF_8);
    fdoRecord.add(new HandleAttribute(PID.getIndex(), PID.getAttribute(), pid));

    // 6: PidIssuer
    fdoRecord.add(new HandleAttribute(PID_ISSUER.getIndex(), PID_ISSUER.getAttribute(),
        TO_BE_FIXED.getBytes(StandardCharsets.UTF_8)));

    // 7: pidIssuerName
    fdoRecord.add(
        new HandleAttribute(PID_ISSUER_NAME.getIndex(), PID_ISSUER_NAME.getAttribute(),
            TO_BE_FIXED.getBytes(
                StandardCharsets.UTF_8)));

    // 8: issuedForAgent -> DiSSCo PID (None Defined atm)
    fdoRecord.add(
        new HandleAttribute(FdoProfileAttributes.ISSUED_FOR_AGENT.getIndex(),
            FdoProfileAttributes.ISSUED_FOR_AGENT.getAttribute(), TO_BE_FIXED.getBytes(
            StandardCharsets.UTF_8)));

    // 9: issuedForAgentName -> DiSSCO
    fdoRecord.add(
        new HandleAttribute(ISSUED_FOR_AGENT_NAME.getIndex(), ISSUED_FOR_AGENT_NAME.getAttribute(),
            "DiSSCo".getBytes(StandardCharsets.UTF_8)));

    // 10: pidRecordIssueDate
    fdoRecord.add(new HandleAttribute(FdoProfileAttributes.PID_RECORD_ISSUE_DATE.getIndex(),
        FdoProfileAttributes.PID_RECORD_ISSUE_DATE.getAttribute(),
        getDate(recordTimeStamp).getBytes(StandardCharsets.UTF_8)));

    // 11: pidRecordIssueNumber
    fdoRecord.add(new HandleAttribute(FdoProfileAttributes.PID_RECORD_ISSUE_NUMBER.getIndex(),
        FdoProfileAttributes.PID_RECORD_ISSUE_NUMBER.getAttribute(),
        "1".getBytes(StandardCharsets.UTF_8)));

    // 12: structuralType
    fdoRecord.add(new HandleAttribute(STRUCTURAL_TYPE.getIndex(),
        STRUCTURAL_TYPE.getAttribute(), "digital".getBytes(StandardCharsets.UTF_8)));

    // 13: PidStatus
    fdoRecord.add(new HandleAttribute(PID_STATUS.getIndex(), PID_STATUS.getAttribute(),
        "DRAFT".getBytes(StandardCharsets.UTF_8)));

    // 40: referentType
    fdoRecord.add(
        new HandleAttribute(REFERENT_TYPE.getIndex(), REFERENT_TYPE.getAttribute(),
            TO_BE_FIXED.getBytes(
                StandardCharsets.UTF_8)));

    // 41: referentDoiName
    fdoRecord.add(
        new HandleAttribute(REFERENT_DOI_NAME.getIndex(), REFERENT_DOI_NAME.getAttribute(),
            handle.getBytes(
                StandardCharsets.UTF_8)));

    // 43: primaryReferentType
    fdoRecord.add(
        new HandleAttribute(PRIMARY_REFERENT_TYPE.getIndex(), PRIMARY_REFERENT_TYPE.getAttribute(),
            "creation".getBytes(StandardCharsets.UTF_8)));

    // 44: referent
    fdoRecord.add(new HandleAttribute(REFERENT.getIndex(), REFERENT.getAttribute(),
        TO_BE_FIXED.getBytes(StandardCharsets.UTF_8)));

    // 210: objectType
    fdoRecord.add(
        new HandleAttribute(OBJECT_TYPE.getIndex(), OBJECT_TYPE.getAttribute(),
            "Digital Specimen".getBytes(StandardCharsets.UTF_8)));

    return fdoRecord;
  }

  private List<HandleAttribute> fillFdoRecordSpecimenAttributes(DigitalSpecimen digitalSpecimen) {
    List<HandleAttribute> fdoRecord = new ArrayList<>();

    // 42: referentName
    var referentName = getAttributeFromDigitalSpecimen(digitalSpecimen,
        ODS_PREFIX + "specimenName");
    referentName.ifPresent(s -> fdoRecord.add(
        new HandleAttribute(REFERENT_NAME.getIndex(), REFERENT_NAME.getAttribute(),
            s.getBytes(StandardCharsets.UTF_8))));

    // 200: SpecimenHost
    var specimenHost = getAttributeFromDigitalSpecimen(digitalSpecimen,
        ODS_PREFIX + "organisationId");
    if (specimenHost.isEmpty()) {
      log.warn("Digital Specimen missing ods:organisationId. Unable to create PID. Check specimen "
          + digitalSpecimen.physicalSpecimenId());
    }

    specimenHost.ifPresent(s -> fdoRecord.add(new HandleAttribute(SPECIMEN_HOST.getIndex(),
        SPECIMEN_HOST.getAttribute(), s.getBytes(StandardCharsets.UTF_8))));

    // 201: Specimen Host Name
    var specimenHostName = getAttributeFromDigitalSpecimen(digitalSpecimen,
        ODS_PREFIX + "organisationName");
    specimenHostName.ifPresent(s -> fdoRecord.add(
        new HandleAttribute(SPECIMEN_HOST_NAME.getIndex(), SPECIMEN_HOST_NAME.getAttribute(),
            s.getBytes(StandardCharsets.UTF_8))));

    // 202: PrimarySpecimenObjectId
    fdoRecord.add(
        new HandleAttribute(PRIMARY_SPECIMEN_OBJECT_ID.getIndex(),
            PRIMARY_SPECIMEN_OBJECT_ID.getAttribute(),
            digitalSpecimen.physicalSpecimenId().getBytes(
                StandardCharsets.UTF_8)));

    // 203: primarySpecimenObjectIdType
    var objectIdType = getAttributeFromDigitalSpecimen(digitalSpecimen,
        ODS_PREFIX + "physicalSpecimenIdType");
    objectIdType.ifPresentOrElse(s -> fdoRecord.add(
            new HandleAttribute(PRIMARY_SPECIMEN_OBJECT_ID_TYPE.getIndex(),
                PRIMARY_SPECIMEN_OBJECT_ID_TYPE.getAttribute(),
                s.getBytes(StandardCharsets.UTF_8))),
        () -> fdoRecord.add(new HandleAttribute(PRIMARY_SPECIMEN_OBJECT_ID_TYPE.getIndex(),
            PRIMARY_SPECIMEN_OBJECT_ID_TYPE.getAttribute(),
            "combined".getBytes(StandardCharsets.UTF_8))));

    // 204: primarySpecimenObjectIdName
    var collectionId = getAttributeFromDigitalSpecimen(digitalSpecimen,
        ODS_PREFIX + "physicalSpecimenCollection");
    if (collectionId.isPresent()) {
      var idName = ("Local identifier for collection " + collectionId).getBytes(
          StandardCharsets.UTF_8);
      fdoRecord.add(new HandleAttribute(PRIMARY_SPECIMEN_OBJECT_ID_NAME.getIndex(),
          PRIMARY_SPECIMEN_OBJECT_ID_NAME.getAttribute(), idName));
    }

    // 209: topicDiscipline
    var topicDiscipline = getAttributeFromDigitalSpecimen(digitalSpecimen,
        ODS_PREFIX + "topicDiscipline");
    topicDiscipline.ifPresent(s -> fdoRecord.add(
        new HandleAttribute(TOPIC_DISCIPLINE.getIndex(), TOPIC_DISCIPLINE.getAttribute(),
            s.getBytes(StandardCharsets.UTF_8))));

    // 216: markedAsType
    var specimenType = getAttributeFromDigitalSpecimen(digitalSpecimen, "dwc:typeStatus");
    if ((specimenType.isPresent() && !NOT_TYPE_STATUS.contains(specimenType.get()))) {
      fdoRecord.add(new HandleAttribute(MARKED_AS_TYPE.getIndex(), MARKED_AS_TYPE.getAttribute(),
          "TRUE".getBytes(StandardCharsets.UTF_8)));
    } else {
      fdoRecord.add(new HandleAttribute(MARKED_AS_TYPE.getIndex(), MARKED_AS_TYPE.getAttribute(),
          "FALSE".getBytes(StandardCharsets.UTF_8)));
    }
    return fdoRecord;
  }

  private Optional<String> getAttributeFromDigitalSpecimen(DigitalSpecimen digitalSpecimen,
      String fieldName) {
    if (digitalSpecimen.attributes().get(fieldName) != null) {
      var attributeVal = digitalSpecimen.attributes().get(fieldName);
      return attributeVal.isNull() ? Optional.empty() : Optional.of(attributeVal.asText());
    } else {
      return Optional.empty();
    }
  }

  private String getDate(Instant recordTimestamp) {
    return dt.format(recordTimestamp);
  }

  private byte[] createLocations(String handle) throws TransformerException {
    var document = documentBuilder.newDocument();
    var locations = document.createElement("locations");
    document.appendChild(locations);
    String[] defaultLocations = new String[]{
        "https://sandbox.dissco.tech/api/v1/specimens/" + handle,
        "https://sandbox.dissco.tech/ds/" + handle
    };
    for (int i = 0; i < defaultLocations.length; i++) {
      var locs = document.createElement("location");
      locs.setAttribute("id", String.valueOf(i));
      locs.setAttribute("href", defaultLocations[i]);
      String weight = i < 1 ? "1" : "0";
      locs.setAttribute("weight", weight);
      locations.appendChild(locs);
    }
    return documentToString(document).getBytes(StandardCharsets.UTF_8);
  }

  private String documentToString(Document document) throws TransformerException {
    var transformer = transformerFactory.newTransformer();
    transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
    StringWriter writer = new StringWriter();
    transformer.transform(new DOMSource(document), new StreamResult(writer));
    return writer.getBuffer().toString();
  }

  private String generateHandle() {
    return PREFIX + newSuffix();
  }

  private String newSuffix() {
    for (int idx = 0; idx < buffer.length; ++idx) {
      if (idx == 3 || idx == 7) { //
        buffer[idx] = '-'; // Sneak a lil dash in the middle
      } else {
        buffer[idx] = symbols[random.nextInt(symbols.length)];
      }
    }
    return new String(buffer);
  }

  private static byte[] decodeAdmin() {
    var admin = "0fff000000153330303a302e4e412f32302e353030302e31303235000000c8";
    byte[] adminByte = new byte[admin.length() / 2];
    for (int i = 0; i < admin.length(); i += 2) {
      adminByte[i / 2] = hexToByte(admin.substring(i, i + 2));
    }
    return adminByte;
  }

  private static byte hexToByte(String hexString) {
    int firstDigit = toDigit(hexString.charAt(0));
    int secondDigit = toDigit(hexString.charAt(1));
    return (byte) ((firstDigit << 4) + secondDigit);
  }

  private static int toDigit(char hexChar) {
    return Character.digit(hexChar, 16);
  }

  private void updateHandle(String id, DigitalSpecimen digitalSpecimen) {
    var handleAttributes = fillFdoRecordSpecimenAttributes(digitalSpecimen);
    var recordTimestamp = Instant.now();
    repository.updateHandleAttributes(id, recordTimestamp, handleAttributes, true);
  }

  public void updateHandles(List<UpdatedDigitalSpecimenTuple> handleUpdates) {
    for (var handleUpdate : handleUpdates) {
      updateHandle(handleUpdate.currentSpecimen().id(), handleUpdate.digitalSpecimenEvent()
          .digitalSpecimen());
    }
  }

  public void rollbackHandleCreation(DigitalSpecimenRecord digitalSpecimenRecord) {
    repository.rollbackHandleCreation(digitalSpecimenRecord.id());
  }

  public void deleteVersion(DigitalSpecimenRecord currentDigitalSpecimen) {
    var handleAttributes = fillFdoRecordSpecimenAttributes(
        currentDigitalSpecimen.digitalSpecimen());
    repository.updateHandleAttributes(currentDigitalSpecimen.id(), Instant.now(), handleAttributes,
        false);
  }
}
