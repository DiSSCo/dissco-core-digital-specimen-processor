package eu.dissco.core.digitalspecimenprocessor.property;

import jakarta.validation.constraints.NotBlank;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Data
@Validated
@ConfigurationProperties("fdo")
public class FdoProperties {

  @NotBlank
  private String specimenType = "https://doi.org/21.T11148/894b1e6cad57e921764e";

  @NotBlank
  private String mediaType = "https://doi.org/21.T11148/bbad8c4e101e8af01115";

}
