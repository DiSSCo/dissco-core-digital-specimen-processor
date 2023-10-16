package eu.dissco.core.digitalspecimenprocessor.property;

import jakarta.annotation.PostConstruct;
import jakarta.validation.constraints.NotBlank;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.validation.annotation.Validated;

@Data
@Validated
@ConfigurationProperties("auth")
public class TokenProperties {

  @NotBlank
  private String secret;

  @NotBlank
  private String id;

  @NotBlank
  private String grantType;

  private MultiValueMap<String, String> fromFormData;

  @PostConstruct
  private void setProperties() {
    fromFormData = new LinkedMultiValueMap<>();
    fromFormData.add("grant_type", grantType);
    fromFormData.add("client_id", id);
    fromFormData.add("client_secret", secret);
  }

}
