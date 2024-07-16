package eu.dissco.core.digitalspecimenprocessor.component;

import eu.dissco.core.digitalspecimenprocessor.repository.SourceSystemRepository;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@AllArgsConstructor
public class SourceSystemNameComponent {

  private final SourceSystemRepository repository;

  @Cacheable("source-system-name")
  public String getSourceSystemName(String sourceSystemID) {
    log.info("Look up for source system with id: {}", sourceSystemID);
    return repository.retrieveNameByID(sourceSystemID.replace("https://hdl.handle.net/", ""));
  }
}
