package eu.dissco.core.digitalspecimenprocessor.property;

import javax.validation.constraints.NotBlank;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Data
@Validated
@ConfigurationProperties("kafka.publisher")
public class KafkaPublisherProperties {

  @NotBlank
  private String host;

}
