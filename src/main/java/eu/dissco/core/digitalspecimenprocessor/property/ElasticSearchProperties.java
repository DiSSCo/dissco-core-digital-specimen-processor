package eu.dissco.core.digitalspecimenprocessor.property;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.Positive;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Data
@Validated
@ConfigurationProperties("elasticsearch")
public class ElasticSearchProperties {

  @NotBlank
  private String hostname;

  @Positive
  private int port;

  @NotBlank
  private String indexName;

}
