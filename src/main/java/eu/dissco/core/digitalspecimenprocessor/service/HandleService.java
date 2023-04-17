package eu.dissco.core.digitalspecimenprocessor.service;

import static eu.dissco.core.digitalspecimenprocessor.database.jooq.Tables.HANDLES;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoUtils.DIGITAL_OBJECT_NAME;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoUtils.DIGITAL_OBJECT_TYPE;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoUtils.FDO_PROFILE;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoUtils.FDO_RECORD_LICENSE;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoUtils.FIELD_IDX;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoUtils.HS_ADMIN;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoUtils.ISSUED_FOR_AGENT;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoUtils.ISSUED_FOR_AGENT_NAME;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoUtils.LIVING_OR_PRESERVED;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoUtils.LOC;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoUtils.MARKED_AS_TYPE;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoUtils.OBJECT_TYPE;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoUtils.OTHER_SPECIMEN_IDS;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoUtils.PID;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoUtils.PID_ISSUER;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoUtils.PID_ISSUER_NAME;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoUtils.PID_RECORD_ISSUE_DATE;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoUtils.PID_RECORD_ISSUE_NUMBER;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoUtils.PID_STATUS;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoUtils.PRIMARY_REFERENT_TYPE;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoUtils.PRIMARY_SPECIMEN_OBJECT_ID;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoUtils.PRIMARY_SPECIMEN_OBJECT_ID_NAME;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoUtils.PRIMARY_SPECIMEN_OBJECT_ID_TYPE;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoUtils.REFERENT;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoUtils.REFERENT_DOI_NAME;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoUtils.REFERENT_NAME;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoUtils.REFERENT_TYPE;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoUtils.SPECIMEN_HOST_NAME;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoUtils.STRUCTURAL_TYPE;

import eu.dissco.core.digitalspecimenprocessor.domain.DigitalSpecimen;
import eu.dissco.core.digitalspecimenprocessor.domain.DigitalSpecimenRecord;
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
import java.util.Random;
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
  private static final HashSet<String> notTypes = new HashSet<>(
      Arrays.asList("false", "specimen", "type"));

  private static final String ODS_PREFIX = "ods:";

  private static final String UNKNOWN = "Unknown";

  private static final String PREFIX = "20.5000.1025/";
  private static final String SPECIMEN_HOST = "specimenHost";
  private static final String TO_BE_FIXED = "Needs to be fixed!";
  private final Random random;
  private final char[] symbols = "ABCDEFGHJKLMNPQRSTVWXYZ1234567890".toCharArray();
  private final char[] buffer = new char[11];
  private final DocumentBuilder documentBuilder;
  private final HandleRepository repository;
  private final TransformerFactory transformerFactory;
  private final DateTimeFormatter dt = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS",
      Locale.ENGLISH).withZone(ZoneId.of("UTC"));

  public String createNewHandle(DigitalSpecimen digitalSpecimen)
      throws TransformerException, PidCreationException {
    checkForPrimarySpecimenObjectId(digitalSpecimen);
    var handle = generateHandle();
    var recordTimestamp = Instant.now();
    var handleAttributes = fillFdoRecord(digitalSpecimen, handle, recordTimestamp);
    repository.createHandle(handle, recordTimestamp, handleAttributes);
    return handle;
  }

  private void checkForPrimarySpecimenObjectId(DigitalSpecimen digitalSpecimen)
      throws PidCreationException {
    var primarySpecimenObjectId = digitalSpecimen.physicalSpecimenId()
        .getBytes(StandardCharsets.UTF_8);
    var optionalRecord = repository.searchByPrimarySpecimenObjectId(primarySpecimenObjectId);
    if (optionalRecord.isPresent()) {
      throw new PidCreationException(
          String.format("Unable to create handle record for specimen with local identifier %s. "
                  + "Local identifier already has persistent identifier: %s",
              digitalSpecimen.physicalSpecimenId(),
              new String(optionalRecord.get().get(HANDLES.HANDLE), StandardCharsets.UTF_8)));
    }
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
        new HandleAttribute(FIELD_IDX.get(HS_ADMIN), HS_ADMIN, decodeAdmin()));

    // 101: 10320/loc
    byte[] loc = createLocations(handle);
    fdoRecord.add(new HandleAttribute(FIELD_IDX.get(LOC), LOC, loc));

    // 1: FDO Profile
    fdoRecord.add(
        new HandleAttribute(FIELD_IDX.get(FDO_PROFILE), FDO_PROFILE, FDO_PROFILE_LOC.getBytes(
            StandardCharsets.UTF_8)));

    // 2: FDO Record License
    fdoRecord.add(new HandleAttribute(FIELD_IDX.get(FDO_RECORD_LICENSE),
        FDO_RECORD_LICENSE, FDO_RECORD_LICENSE_LOC));

    // 3: DigitalObjectType
    fdoRecord.add(
        new HandleAttribute(FIELD_IDX.get(DIGITAL_OBJECT_TYPE), DIGITAL_OBJECT_TYPE,
            DIGITAL_OBJECT_TYPE_LOC.getBytes(
                StandardCharsets.UTF_8)));

    // 4: DigitalObjectName
    fdoRecord.add(
        new HandleAttribute(FIELD_IDX.get(DIGITAL_OBJECT_NAME), DIGITAL_OBJECT_NAME,
            DIGITAL_OBJECT_TYPE_VALUE));

    // 5: Pid
    byte[] pid = (HANDLE_PROXY + handle).getBytes(StandardCharsets.UTF_8);
    fdoRecord.add(new HandleAttribute(FIELD_IDX.get(PID), PID, pid));

    // 6: PidIssuer
    fdoRecord.add(new HandleAttribute(FIELD_IDX.get(PID_ISSUER), PID_ISSUER,
        TO_BE_FIXED.getBytes(StandardCharsets.UTF_8)));

    // 7: pidIssuerName
    fdoRecord.add(
        new HandleAttribute(FIELD_IDX.get(PID_ISSUER_NAME), PID_ISSUER_NAME, TO_BE_FIXED.getBytes(
            StandardCharsets.UTF_8)));

    // 8: issuedForAgent -> DiSSCo PID (None Defined atm)
    fdoRecord.add(
        new HandleAttribute(FIELD_IDX.get(ISSUED_FOR_AGENT), ISSUED_FOR_AGENT, TO_BE_FIXED.getBytes(
            StandardCharsets.UTF_8)));

    // 9: issuedForAgentName -> DiSSCO
    fdoRecord.add(
        new HandleAttribute(FIELD_IDX.get(ISSUED_FOR_AGENT_NAME), ISSUED_FOR_AGENT_NAME,
            "DiSSCo".getBytes(StandardCharsets.UTF_8)));

    // 10: pidRecordIssueDate
    fdoRecord.add(new HandleAttribute(FIELD_IDX.get(PID_RECORD_ISSUE_DATE),
        PID_RECORD_ISSUE_DATE, getDate(recordTimeStamp).getBytes(StandardCharsets.UTF_8)));

    // 11: pidRecordIssueNumber
    fdoRecord.add(new HandleAttribute(FIELD_IDX.get(PID_RECORD_ISSUE_NUMBER),
        PID_RECORD_ISSUE_NUMBER, "1".getBytes(StandardCharsets.UTF_8)));

    // 12: structuralType
    fdoRecord.add(new HandleAttribute(FIELD_IDX.get(STRUCTURAL_TYPE),
        STRUCTURAL_TYPE, "digital".getBytes(StandardCharsets.UTF_8)));

    // 13: PidStatus
    fdoRecord.add(new HandleAttribute(FIELD_IDX.get(PID_STATUS), PID_STATUS,
        "DRAFT".getBytes(StandardCharsets.UTF_8)));

    // 40: referentType
    fdoRecord.add(
        new HandleAttribute(FIELD_IDX.get(REFERENT_TYPE), REFERENT_TYPE, TO_BE_FIXED.getBytes(
            StandardCharsets.UTF_8)));

    // 41: referentDoiName
    fdoRecord.add(
        new HandleAttribute(FIELD_IDX.get(REFERENT_DOI_NAME), REFERENT_DOI_NAME, handle.getBytes(
            StandardCharsets.UTF_8)));

    // 43: primaryReferentType
    fdoRecord.add(
        new HandleAttribute(FIELD_IDX.get(PRIMARY_REFERENT_TYPE), PRIMARY_REFERENT_TYPE,
            "creation".getBytes(StandardCharsets.UTF_8)));

    // 44: referent
    fdoRecord.add(new HandleAttribute(FIELD_IDX.get(REFERENT), REFERENT,
        TO_BE_FIXED.getBytes(StandardCharsets.UTF_8)));

    // 210: objectType
    fdoRecord.add(
        new HandleAttribute(FIELD_IDX.get(OBJECT_TYPE), OBJECT_TYPE,
            "Digital Specimen".getBytes(StandardCharsets.UTF_8)));

    return fdoRecord;
  }

  private List<HandleAttribute> fillFdoRecordSpecimenAttributes(DigitalSpecimen digitalSpecimen) {
    List<HandleAttribute> fdoRecord = new ArrayList<>();
    // 42: referentName
    fdoRecord.add(
        new HandleAttribute(FIELD_IDX.get(REFERENT_NAME), REFERENT_NAME,
            getAttributeFromDigitalSpecimen(digitalSpecimen, ODS_PREFIX + "specimenName",
                UNKNOWN).getBytes(StandardCharsets.UTF_8)));

    // 200: SpecimenHost
    fdoRecord.add(
        new HandleAttribute(FIELD_IDX.get(SPECIMEN_HOST),
            SPECIMEN_HOST,
            getAttributeFromDigitalSpecimen(digitalSpecimen, ODS_PREFIX + "organisationId",
                UNKNOWN).getBytes(StandardCharsets.UTF_8)));

    // 201: Specimen Host Name
    fdoRecord.add(
        new HandleAttribute(FIELD_IDX.get(SPECIMEN_HOST_NAME), SPECIMEN_HOST_NAME,
            getAttributeFromDigitalSpecimen(digitalSpecimen, ODS_PREFIX + "organisationName",
                UNKNOWN).getBytes(StandardCharsets.UTF_8)));

    // 202: PrimarySpecimenObjectId
    fdoRecord.add(
        new HandleAttribute(FIELD_IDX.get(PRIMARY_SPECIMEN_OBJECT_ID),
            PRIMARY_SPECIMEN_OBJECT_ID,
            getAttributeFromDigitalSpecimen(digitalSpecimen, ODS_PREFIX + "physicalSpecimenId",
                UNKNOWN).getBytes(
                StandardCharsets.UTF_8)));

    // 203: primarySpecimenObjectIdType
    fdoRecord.add(
        new HandleAttribute(FIELD_IDX.get(PRIMARY_SPECIMEN_OBJECT_ID_TYPE),
            PRIMARY_SPECIMEN_OBJECT_ID_TYPE,
            getAttributeFromDigitalSpecimen(digitalSpecimen, ODS_PREFIX + "organisationName",
                UNKNOWN).getBytes(StandardCharsets.UTF_8)));

    // 204: primarySpecimenObjectIdName
    var collectionId = getAttributeFromDigitalSpecimen(digitalSpecimen,
        ODS_PREFIX + "physicalSpecimenCollection", "");
    if (!collectionId.isEmpty()) {
      var idName = ("Local identifier for collection " + collectionId).getBytes(
          StandardCharsets.UTF_8);
      fdoRecord.add(new HandleAttribute(FIELD_IDX.get(PRIMARY_SPECIMEN_OBJECT_ID_NAME),
          PRIMARY_SPECIMEN_OBJECT_ID_NAME, idName));
    }

    // 206: otherSpecimenIds
    var otherSpecimenIds = getAttributeFromDigitalSpecimen(digitalSpecimen, "dwca:id", "");
    if (!otherSpecimenIds.isEmpty()) {
      fdoRecord.add(new HandleAttribute(FIELD_IDX.get(OTHER_SPECIMEN_IDS), OTHER_SPECIMEN_IDS,
          otherSpecimenIds.getBytes(
              StandardCharsets.UTF_8)));
    }

    // 211: livingOrPreserved
    var livingOrPreserved = getUnharmonisedAttributeFromDigitalSpecimen(digitalSpecimen,
        "dwca:basisOfRecord",
        "");
    if (!livingOrPreserved.isEmpty()) {
      fdoRecord.add(new HandleAttribute(FIELD_IDX.get(LIVING_OR_PRESERVED), LIVING_OR_PRESERVED,
          livingOrPreserved.getBytes(
              StandardCharsets.UTF_8)));
    }

    // 216: markedAsType
    var specimenType = getAttributeFromDigitalSpecimen(digitalSpecimen, ODS_PREFIX + "typeStatus",
        "");
    if (!(specimenType.isEmpty() && notTypes.contains(specimenType))) {
      fdoRecord.add(new HandleAttribute(FIELD_IDX.get(MARKED_AS_TYPE), MARKED_AS_TYPE,
          "TRUE".getBytes(StandardCharsets.UTF_8)));
    } else {
      fdoRecord.add(new HandleAttribute(FIELD_IDX.get(MARKED_AS_TYPE), MARKED_AS_TYPE,
          "FALSE".getBytes(StandardCharsets.UTF_8)));
    }
    return fdoRecord;
  }

  private String getAttributeFromDigitalSpecimen(DigitalSpecimen digitalSpecimen, String fieldName,
      String notFoundDefault) {
    if (digitalSpecimen.attributes().get(fieldName) != null) {
      return digitalSpecimen.attributes().get(fieldName).asText();
    } else {
      return notFoundDefault;
    }
  }

  private String getUnharmonisedAttributeFromDigitalSpecimen(DigitalSpecimen digitalSpecimen,
      String fieldName, String notFoundDefault) {
    if (digitalSpecimen.originalAttributes().get(fieldName) != null) {
      return digitalSpecimen.attributes().get(fieldName).asText();
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
    var firstLocation = document.createElement("location");
    firstLocation.setAttribute("id", "0");
    firstLocation.setAttribute("href", "https://sandbox.dissco.tech/api/v1/specimens/" + handle);
    firstLocation.setAttribute("weight", "0");
    locations.appendChild(firstLocation);
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

  private byte[] decodeAdmin() {
    var admin = "0fff000000153330303a302e4e412f32302e353030302e31303235000000c8";
    byte[] adminByte = new byte[admin.length() / 2];
    for (int i = 0; i < admin.length(); i += 2) {
      adminByte[i / 2] = hexToByte(admin.substring(i, i + 2));
    }
    return adminByte;
  }

  private byte hexToByte(String hexString) {
    int firstDigit = toDigit(hexString.charAt(0));
    int secondDigit = toDigit(hexString.charAt(1));
    return (byte) ((firstDigit << 4) + secondDigit);
  }

  private int toDigit(char hexChar) {
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
