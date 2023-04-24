package eu.dissco.core.digitalspecimenprocessor.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dissco.core.digitalspecimenprocessor.Profiles;
import eu.dissco.core.digitalspecimenprocessor.domain.DigitalSpecimen;
import eu.dissco.core.digitalspecimenprocessor.domain.DigitalSpecimenEvent;
import eu.dissco.core.digitalspecimenprocessor.domain.DigitalSpecimenEventOld;
import eu.dissco.core.digitalspecimenprocessor.domain.DigitalSpecimenRecord;
import eu.dissco.core.digitalspecimenprocessor.exception.NoChangesFoundException;
import eu.dissco.core.digitalspecimenprocessor.service.ProcessingService;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@Profile(Profiles.WEB)
@RestController
@RequestMapping("/")
@RequiredArgsConstructor
public class DigitalSpecimenController {

  private final ProcessingService processingService;
  private final ObjectMapper mapper;

  @PreAuthorize("isAuthenticated()")
  @PostMapping(consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<DigitalSpecimenRecord> upsertDigitalSpecimen(@RequestBody
  DigitalSpecimenEventOld event) throws NoChangesFoundException {
    log.info("Received digitalSpecimen upsert on old endpoint: {}", event);
    return upsertDigitalSpecimen(mapToNewDigitalSpecimenFormat(event));
  }

  @PreAuthorize("isAuthenticated()")
  @PostMapping(value = "new", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<DigitalSpecimenRecord> upsertDigitalSpecimen(@RequestBody
  DigitalSpecimenEvent event) throws NoChangesFoundException {
    log.info("Received digitalSpecimen upsert: {}", event);
    var result = processingService.handleMessages(List.of(event));
    if (result.isEmpty()) {
      throw new NoChangesFoundException("No changes found for specimen");
    }
    return ResponseEntity.status(HttpStatus.CREATED).body(result.get(0));
  }

  private DigitalSpecimenEvent mapToNewDigitalSpecimenFormat(DigitalSpecimenEventOld event) {
    var digtalSpecimenOld = event.digitalSpecimen();
    var attributes = mapper.createObjectNode();
    attributes.put("ods:physicalSpecimenIdType", digtalSpecimenOld.physicalSpecimenIdType());
    attributes.put("ods:organisationId", digtalSpecimenOld.organizationId());
    attributes.put("ods:specimenName", digtalSpecimenOld.specimenName());
    attributes.put("ods:datasetId", digtalSpecimenOld.datasetId());
    attributes.put("ods:physicalSpecimenCollection",
        digtalSpecimenOld.physicalSpecimenCollection());
    attributes.put("ods:sourceSystemId", digtalSpecimenOld.sourceSystemId());
    attributes.put("dwca:id", digtalSpecimenOld.dwcaId());
    return new DigitalSpecimenEvent(event.enrichmentList(), new DigitalSpecimen(
        digtalSpecimenOld.physicalSpecimenId(),
        digtalSpecimenOld.type(),
        attributes,
        digtalSpecimenOld.originalData()
    ));
  }

  @ExceptionHandler(NoChangesFoundException.class)
  public ResponseEntity<String> handleException(NoChangesFoundException e) {
    return ResponseEntity.status(HttpStatus.OK).body(e.getMessage());
  }
}
