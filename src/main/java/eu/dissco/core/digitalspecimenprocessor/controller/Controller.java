package eu.dissco.core.digitalspecimenprocessor.controller;

import eu.dissco.core.digitalspecimenprocessor.Profiles;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaEvent;
import eu.dissco.core.digitalspecimenprocessor.domain.media.MediaProcessResult;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenEvent;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.SpecimenProcessResult;
import eu.dissco.core.digitalspecimenprocessor.exception.NoChangesFoundException;
import eu.dissco.core.digitalspecimenprocessor.service.ProcessingService;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
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
public class Controller {

  private final ProcessingService processingService;

  @PostMapping(value = "", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<SpecimenProcessResult> upsertDigitalSpecimen(@RequestBody
  DigitalSpecimenEvent event) throws NoChangesFoundException {
    log.info("Received digitalSpecimenWrapper upsert: {}", event);
    var result = processingService.handleMessages(List.of(event));
    if (result.newDigitalSpecimens().isEmpty() &&
        result.updatedDigitalSpecimens().isEmpty()) {
      throw new NoChangesFoundException("No changes found for specimen");
    }
    return ResponseEntity.status(HttpStatus.CREATED).body(result);
  }

  @PostMapping(value = "media", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<MediaProcessResult> upsertDigitalMedia(@RequestBody
  DigitalMediaEvent event) throws NoChangesFoundException {
    log.info("Received digitalMedia upsert: {}", event);
    var result = processingService.handleMessagesMedia(List.of(event));
    if (result.newMedia().isEmpty() && result.updatedMedia().isEmpty()) {
      throw new NoChangesFoundException("No changes found for media");
    }
    return ResponseEntity.status(HttpStatus.CREATED).body(result);
  }

  @ExceptionHandler(NoChangesFoundException.class)
  public ResponseEntity<String> handleException(NoChangesFoundException e) {
    return ResponseEntity.status(HttpStatus.OK).body(e.getMessage());
  }
}
