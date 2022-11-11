package eu.dissco.core.digitalspecimenprocessor.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dissco.core.digitalspecimenprocessor.domain.DigitalSpecimen;
import eu.dissco.core.digitalspecimenprocessor.domain.DigitalSpecimenRecord;
import eu.dissco.core.digitalspecimenprocessor.domain.HandleAttribute;
import eu.dissco.core.digitalspecimenprocessor.domain.UpdatedDigitalSpecimenTuple;
import eu.dissco.core.digitalspecimenprocessor.repository.HandleRepository;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
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

  private static final String PREFIX = "20.5000.1025/";
  private static final String HANDLE = "Handle";
  private static final String DIGITAL_OBJECT_SUBTYPE = "digitalObjectSubtype";
  private static final String SPECIMEN_HOST = "specimenHost";
  private static final String TO_BE_FIXED = "Needs to be fixed!";
  private static final String DUMMY_HANDLE = "http://hdl.handle.net/21...";
  private final Random random;
  private final char[] symbols = "ABCDEFGHJKLMNPQRSTVWXYZ1234567890".toCharArray();
  private final char[] buffer = new char[11];
  private final ObjectMapper mapper;
  private final DocumentBuilder documentBuilder;
  private final HandleRepository repository;
  private final TransformerFactory transformerFactory;

  public String createNewHandle(DigitalSpecimen digitalSpecimen)
      throws TransformerException {
    var handle = generateHandle();
    var recordTimestamp = Instant.now();
    var handleAttributes = fillPidRecord(digitalSpecimen, handle, recordTimestamp);
    repository.createHandle(handle, recordTimestamp, handleAttributes);
    return handle;
  }

  private List<HandleAttribute> fillPidRecord(DigitalSpecimen digitalSpecimen, String handle,
      Instant recordTimestamp)
      throws TransformerException {
    var handleAttributes = new ArrayList<HandleAttribute>();
    handleAttributes.add(
        new HandleAttribute(1, "pid",
            ("https://hdl.handle.net/" + handle).getBytes(StandardCharsets.UTF_8)));
    handleAttributes.add(new HandleAttribute(2, "pidIssuer",
        createPidReference("https://doi.org/10.22/10.22/2AA-GAA-E29", "DOI", "RA Issuing DOI")));
    handleAttributes.add(new HandleAttribute(3, "digitalObjectType",
        createPidReference(DUMMY_HANDLE, HANDLE, "Digital Specimen")));
    handleAttributes.add(new HandleAttribute(4, DIGITAL_OBJECT_SUBTYPE,
        createPidReference("https://hdl.handle.net/21...", HANDLE, digitalSpecimen.type())));
    handleAttributes.add(new HandleAttribute(5, "10320/loc", createLocations(handle)));
    handleAttributes.add(new HandleAttribute(6, "issueDate", createIssueDate(recordTimestamp)));
    handleAttributes.add(
        new HandleAttribute(7, "issueNumber", "1".getBytes(StandardCharsets.UTF_8)));
    handleAttributes.add(
        new HandleAttribute(8, "pidStatus", "DRAFT".getBytes(StandardCharsets.UTF_8)));
    handleAttributes.add(
        new HandleAttribute(11, "pidKernelMetadataLicense",
            "https://creativecommons.org/publicdomain/zero/1.0/".getBytes(StandardCharsets.UTF_8)));
    handleAttributes.add(new HandleAttribute(14, "digitalOrPhysical", "physical".getBytes(
        StandardCharsets.UTF_8)));
    handleAttributes.add(new HandleAttribute(15, SPECIMEN_HOST, createPidReference(
        digitalSpecimen.organizationId(), "ROR", TO_BE_FIXED)));
    handleAttributes.add(new HandleAttribute(100, "HS_ADMIN", decodeAdmin()));
    return handleAttributes;
  }

  private byte[] createIssueDate(Instant recordTimestamp) {
    SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
    return formatter.format(Date.from(recordTimestamp)).getBytes(StandardCharsets.UTF_8);
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

  private byte[] createPidReference(String pid, String pidType, String primaryNameFromPid) {
    var objectNode = mapper.createObjectNode();
    objectNode.put("id", pid);
    objectNode.put("pidType", pidType);
    objectNode.put("primaryNameFromPid", primaryNameFromPid);
    return objectNode.toString().getBytes(StandardCharsets.UTF_8);
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
    var handleAttributes = updatedHandles(digitalSpecimen);
    var recordTimestamp = Instant.now();
    repository.updateHandleAttributes(id, recordTimestamp, handleAttributes, true);
  }

  private List<HandleAttribute> updatedHandles(DigitalSpecimen digitalSpecimen) {
    var handleAttributes = new ArrayList<HandleAttribute>();
    handleAttributes.add(new HandleAttribute(4, DIGITAL_OBJECT_SUBTYPE,
        createPidReference(DUMMY_HANDLE, HANDLE, digitalSpecimen.type())));
    handleAttributes.add(new HandleAttribute(15, SPECIMEN_HOST, createPidReference(
        digitalSpecimen.organizationId(), "ROR", TO_BE_FIXED)));
    return handleAttributes;
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
    var handleAttributes = new ArrayList<HandleAttribute>();
    handleAttributes.add(new HandleAttribute(4, DIGITAL_OBJECT_SUBTYPE,
        createPidReference(DUMMY_HANDLE, HANDLE, currentDigitalSpecimen.digitalSpecimen().type())));
    handleAttributes.add(new HandleAttribute(15, SPECIMEN_HOST, createPidReference(
        currentDigitalSpecimen.digitalSpecimen().organizationId(), "ROR", TO_BE_FIXED)));
    handleAttributes.add(
        new HandleAttribute(6, "issueDate", createIssueDate(currentDigitalSpecimen.created())));
    repository.updateHandleAttributes(currentDigitalSpecimen.id(), Instant.now(), handleAttributes,
        false);
  }
}
