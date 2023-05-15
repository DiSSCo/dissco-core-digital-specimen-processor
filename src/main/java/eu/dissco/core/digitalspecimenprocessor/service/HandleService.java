package eu.dissco.core.digitalspecimenprocessor.service;

import static eu.dissco.core.digitalspecimenprocessor.database.jooq.Tables.HANDLES;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfile.DIGITAL_OBJECT_NAME;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfile.FDO_PROFILE;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfile.FDO_RECORD_LICENSE;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfile.HS_ADMIN;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfile.ISSUED_FOR_AGENT_NAME;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfile.LIVING_OR_PRESERVED;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfile.LOC;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfile.MARKED_AS_TYPE;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfile.OBJECT_TYPE;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfile.OTHER_SPECIMEN_IDS;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfile.PID;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfile.PID_ISSUER;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfile.PID_ISSUER_NAME;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfile.PID_STATUS;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfile.PRIMARY_REFERENT_TYPE;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfile.PRIMARY_SPECIMEN_OBJECT_ID;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfile.PRIMARY_SPECIMEN_OBJECT_ID_NAME;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfile.PRIMARY_SPECIMEN_OBJECT_ID_TYPE;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfile.REFERENT;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfile.REFERENT_DOI_NAME;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfile.REFERENT_NAME;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfile.REFERENT_TYPE;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfile.SPECIMEN_HOST_NAME;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfile.STRUCTURAL_TYPE;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfile.SPECIMEN_HOST;

import eu.dissco.core.digitalspecimenprocessor.domain.DigitalSpecimen;
import eu.dissco.core.digitalspecimenprocessor.domain.DigitalSpecimenRecord;
import eu.dissco.core.digitalspecimenprocessor.domain.FdoProfile;
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
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
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
import org.springframework.stereotype.Service;
import org.w3c.dom.Document;

@Slf4j
@Service
@RequiredArgsConstructor
public class HandleService {

  private static final String HANDLE_PROXY = "https://hdl.handle.net/";
  private static final String FDO_PROFILE_LOC = HANDLE_PROXY + "21.T11148/d8de0819e144e4096645";
  private static final String DIGITAL_OBJECT_TYPE_LOC =
      HANDLE_PROXY + "21.T11148/894b1e6cad57e921764e";
  private static final byte[] DIGITAL_OBJECT_TYPE_VALUE = "digitalSpecimen".getBytes(
      StandardCharsets.UTF_8);
  private static final byte[] FDO_RECORD_LICENSE_LOC = "https://creativecommons.org/publicdomain/zero/1.0/".getBytes(
      StandardCharsets.UTF_8);
  private static final Set<String> notTypes = new HashSet<>(
      Arrays.asList("false", "specimen", "type"));

  private static final String ODS_PREFIX = "ods:";

  private static final String UNKNOWN = "Unknown";

  private static final String PREFIX = "20.5000.1025/";
  private static final String TO_BE_FIXED = "Needs to be fixed!";
  private final char[] symbols = "ABCDEFGHJKLMNPQRSTVWXYZ1234567890".toCharArray();
  private final char[] buffer = new char[11];
  private final Random random;
  private final DocumentBuilder documentBuilder;
  private final HandleRepository repository;
  private final TransformerFactory transformerFactory;
  private final DateTimeFormatter dt = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX").withZone(ZoneId.of("UTC"));

  public String createNewHandle(DigitalSpecimen digitalSpecimen)
      throws TransformerException, PidCreationException {
    var existingHandle = checkForPrimarySpecimenObjectId(digitalSpecimen);
    if(existingHandle.isPresent()){
      log.info("Digital specimen with id {} already exists under handle {}. Updating FDO Record.", digitalSpecimen.physicalSpecimenId(), existingHandle.get());
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
        new HandleAttribute(FDO_PROFILE.getIndex(), FDO_PROFILE.getAttribute(), FDO_PROFILE_LOC.getBytes(
            StandardCharsets.UTF_8)));

    // 2: FDO Record License
    fdoRecord.add(new HandleAttribute(FDO_RECORD_LICENSE.getIndex(),
        FDO_RECORD_LICENSE.getAttribute(), FDO_RECORD_LICENSE_LOC));

    // 3: DigitalObjectType
    fdoRecord.add(
        new HandleAttribute(FdoProfile.DIGITAL_OBJECT_TYPE.getIndex(),
            FdoProfile.DIGITAL_OBJECT_TYPE.getAttribute(),
            DIGITAL_OBJECT_TYPE_LOC.getBytes(
                StandardCharsets.UTF_8)));

    // 4: DigitalObjectName
    fdoRecord.add(
        new HandleAttribute(DIGITAL_OBJECT_NAME.getIndex(), DIGITAL_OBJECT_NAME.getAttribute(),
            DIGITAL_OBJECT_TYPE_VALUE));

    // 5: Pid
    byte[] pid = (HANDLE_PROXY + handle).getBytes(StandardCharsets.UTF_8);
    fdoRecord.add(new HandleAttribute(PID.getIndex(), PID.getAttribute(), pid));

    // 6: PidIssuer
    fdoRecord.add(new HandleAttribute(PID_ISSUER.getIndex(), PID_ISSUER.getAttribute(),
        TO_BE_FIXED.getBytes(StandardCharsets.UTF_8)));

    // 7: pidIssuerName
    fdoRecord.add(
        new HandleAttribute(PID_ISSUER_NAME.getIndex(), PID_ISSUER_NAME.getAttribute(), TO_BE_FIXED.getBytes(
            StandardCharsets.UTF_8)));

    // 8: issuedForAgent -> DiSSCo PID (None Defined atm)
    fdoRecord.add(
        new HandleAttribute(FdoProfile.ISSUED_FOR_AGENT.getIndex(),
            FdoProfile.ISSUED_FOR_AGENT.getAttribute(), TO_BE_FIXED.getBytes(
            StandardCharsets.UTF_8)));

    // 9: issuedForAgentName -> DiSSCO
    fdoRecord.add(
        new HandleAttribute(ISSUED_FOR_AGENT_NAME.getIndex(), ISSUED_FOR_AGENT_NAME.getAttribute(),
            "DiSSCo".getBytes(StandardCharsets.UTF_8)));

    // 10: pidRecordIssueDate
    fdoRecord.add(new HandleAttribute(FdoProfile.PID_RECORD_ISSUE_DATE.getIndex(),
        FdoProfile.PID_RECORD_ISSUE_DATE.getAttribute(), getDate(recordTimeStamp).getBytes(StandardCharsets.UTF_8)));

    // 11: pidRecordIssueNumber
    fdoRecord.add(new HandleAttribute(FdoProfile.PID_RECORD_ISSUE_NUMBER.getIndex(),
        FdoProfile.PID_RECORD_ISSUE_NUMBER.getAttribute(), "1".getBytes(StandardCharsets.UTF_8)));

    // 12: structuralType
    fdoRecord.add(new HandleAttribute(STRUCTURAL_TYPE.getIndex(),
        STRUCTURAL_TYPE.getAttribute(), "digital".getBytes(StandardCharsets.UTF_8)));

    // 13: PidStatus
    fdoRecord.add(new HandleAttribute(PID_STATUS.getIndex(), PID_STATUS.getAttribute(),
        "DRAFT".getBytes(StandardCharsets.UTF_8)));

    // 40: referentType
    fdoRecord.add(
        new HandleAttribute(REFERENT_TYPE.getIndex(), REFERENT_TYPE.getAttribute(), TO_BE_FIXED.getBytes(
            StandardCharsets.UTF_8)));

    // 41: referentDoiName
    fdoRecord.add(
        new HandleAttribute(REFERENT_DOI_NAME.getIndex(), REFERENT_DOI_NAME.getAttribute(), handle.getBytes(
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
    fdoRecord.add(
        new HandleAttribute(REFERENT_NAME.getIndex(), REFERENT_NAME.getAttribute(),
            getAttributeFromDigitalSpecimen(digitalSpecimen, ODS_PREFIX + "specimenName",
                UNKNOWN).getBytes(StandardCharsets.UTF_8)));

    // 200: SpecimenHost
    fdoRecord.add(
        new HandleAttribute(SPECIMEN_HOST.getIndex(),
            SPECIMEN_HOST.getAttribute(),
            getAttributeFromDigitalSpecimen(digitalSpecimen, ODS_PREFIX + "organisationId",
                UNKNOWN).getBytes(StandardCharsets.UTF_8)));

    // 201: Specimen Host Name
    fdoRecord.add(
        new HandleAttribute(SPECIMEN_HOST_NAME.getIndex(), SPECIMEN_HOST_NAME.getAttribute(),
            getAttributeFromDigitalSpecimen(digitalSpecimen, ODS_PREFIX + "organisationName",
                UNKNOWN).getBytes(StandardCharsets.UTF_8)));

    // 202: PrimarySpecimenObjectId
    fdoRecord.add(
        new HandleAttribute(PRIMARY_SPECIMEN_OBJECT_ID.getIndex(),
            PRIMARY_SPECIMEN_OBJECT_ID.getAttribute(),
            digitalSpecimen.physicalSpecimenId().getBytes(
                StandardCharsets.UTF_8)));

    // 203: primarySpecimenObjectIdType
    fdoRecord.add(
        new HandleAttribute(PRIMARY_SPECIMEN_OBJECT_ID_TYPE.getIndex(),
            PRIMARY_SPECIMEN_OBJECT_ID_TYPE.getAttribute(),
            getAttributeFromDigitalSpecimen(digitalSpecimen, ODS_PREFIX + "physicalSpecimenIdType",
                UNKNOWN).getBytes(StandardCharsets.UTF_8)));

    // 204: primarySpecimenObjectIdName
    var collectionId = getAttributeFromDigitalSpecimen(digitalSpecimen,
        ODS_PREFIX + "physicalSpecimenCollection", "");
    if (!collectionId.isEmpty()) {
      var idName = ("Local identifier for collection " + collectionId).getBytes(
          StandardCharsets.UTF_8);
      fdoRecord.add(new HandleAttribute(PRIMARY_SPECIMEN_OBJECT_ID_NAME.getIndex(),
          PRIMARY_SPECIMEN_OBJECT_ID_NAME.getAttribute(), idName));
    }

    // 206: otherSpecimenIds
    var otherSpecimenIds = getAttributeFromDigitalSpecimen(digitalSpecimen, "dwca:id", "");
    if (!otherSpecimenIds.isEmpty()) {
      fdoRecord.add(new HandleAttribute(OTHER_SPECIMEN_IDS.getIndex(), OTHER_SPECIMEN_IDS.getAttribute(),
          otherSpecimenIds.getBytes(
              StandardCharsets.UTF_8)));
    }

    // 216: markedAsType
    var specimenType = getAttributeFromDigitalSpecimen(digitalSpecimen, "dwc:typeStatus",
        "");
    if (!(specimenType.isEmpty() || notTypes.contains(specimenType))) {
      fdoRecord.add(new HandleAttribute(MARKED_AS_TYPE.getIndex(), MARKED_AS_TYPE.getAttribute(),
          "TRUE".getBytes(StandardCharsets.UTF_8)));
    } else {
      fdoRecord.add(new HandleAttribute(MARKED_AS_TYPE.getIndex(), MARKED_AS_TYPE.getAttribute(),
          "FALSE".getBytes(StandardCharsets.UTF_8)));
    }
    return fdoRecord;
  }

  private String getAttributeFromDigitalSpecimen(DigitalSpecimen digitalSpecimen, String fieldName,
      String notFoundDefault) {
    if (digitalSpecimen.attributes().get(fieldName) != null) {
      var attributeVal = digitalSpecimen.attributes().get(fieldName);
      return attributeVal.isNull() ? notFoundDefault : attributeVal.asText();
    } else {
      return notFoundDefault;
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
    for (int i = 0; i< defaultLocations.length; i++){
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
