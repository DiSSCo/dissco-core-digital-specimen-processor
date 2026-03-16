package eu.dissco.core.digitalspecimenprocessor.property;

import jakarta.validation.constraints.NotNull;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Data
@Validated
@ConfigurationProperties("annotation")
public class AnnotationProperties {

  @NotNull
  private boolean applyAcceptedAnnotations = false;

}
