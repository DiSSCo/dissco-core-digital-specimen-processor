package eu.dissco.core.digitalspecimenprocessor.property;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Data
@Validated
@ConfigurationProperties(prefix = "rabbitmq")
public class RabbitMQProperties {

  @Positive
  private int batchSize = 500;

  @NotBlank
  private String specimenExchangeName = "digital-specimen-exchange";

  @NotNull
  private String specimenRoutingKeyName = "digital-specimen";

  @NotBlank
  private String specimenQueueName = "digital-specimen-queue";

  @NotBlank
  private String specimenDlqExchangeName = "digital-specimen-exchange-dlq";

  @NotBlank
  private String specimenDlqRoutingKeyName = "digital-specimen-dlq";

  @NotBlank
  private String digitalMediaExchangeName = "digital-media-exchange";

  @NotNull
  private String digitalMediaRoutingKeyName = "digital-media";

  @NotBlank
  private String autoAnnotationExchangeName = "auto-annotation-exchange";

  @NotNull
  private String autoAnnotationRoutingKeyName = "auto-annotation";

  @NotBlank
  private String createUpdateTombstoneExchangeName = "create-update-tombstone-exchange";

  @NotNull
  private String createUpdateTombstoneRoutingKeyName = "create-update-tombstone";

  @NotBlank
  private String masExchangeName = "mas-exchange";



}
