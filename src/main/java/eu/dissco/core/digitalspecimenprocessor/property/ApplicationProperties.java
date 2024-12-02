package eu.dissco.core.digitalspecimenprocessor.property;

import jakarta.validation.constraints.NotBlank;
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
  private String name = "dissco-core-digital-specimen-processor";

  @NotBlank
  private String pid = "https://hdl.handle.net/TEST/123-123-123";

  @NotBlank
  private String createUpdateTombstoneEventType = "https://doi.org/21.T11148/d7570227982f70256af3";

}
