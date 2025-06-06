package eu.dissco.core.digitalspecimenprocessor.property;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Positive;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Data
@Validated
@ConfigurationProperties("application")
public class ApplicationProperties {

  @NotBlank
  private String specimenBaseUrl = "https://doi.org/";

  @NotBlank
  private String name = "DiSSCo Digital Specimen Processing Service";

  @NotBlank
  private String pid = "https://doi.org/10.5281/zenodo.14383054";

  @NotBlank
  private String createUpdateTombstoneEventType = "https://doi.org/21.T11148/d7570227982f70256af3";

  @Positive
  private Integer maxHandles = 1000;

}
