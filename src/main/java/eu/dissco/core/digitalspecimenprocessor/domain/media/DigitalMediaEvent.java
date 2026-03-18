package eu.dissco.core.digitalspecimenprocessor.domain.media;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;
import java.util.Set;

public record DigitalMediaEvent(
    Set<String> masList,
    @JsonProperty("digitalMedia")
    DigitalMediaWrapper digitalMediaWrapper,
    Boolean forceMasSchedule,
    Boolean isDataFromSourceSystem) {

  public DigitalMediaEvent(Set<String> masList, DigitalMediaWrapper digitalMediaWrapper,
      Boolean forceMasSchedule,
      Boolean isDataFromSourceSystem) {
    this.masList = masList;
    this.digitalMediaWrapper = digitalMediaWrapper;
    this.forceMasSchedule = forceMasSchedule;
    this.isDataFromSourceSystem = Objects.requireNonNullElse(isDataFromSourceSystem, Boolean.TRUE);
  }

}
