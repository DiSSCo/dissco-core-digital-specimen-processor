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
public class RabbitMqProperties {

  @Positive
  private int batchSize = 500;
  @NotNull
  private Specimen specimen = new Specimen();
  @NotNull
  private DigitalMedia digitalMedia = new DigitalMedia();
  @NotNull
  private AutoAcceptedAnnotation autoAcceptedAnnotation = new AutoAcceptedAnnotation();
  @NotNull
  private CreateUpdateTombstone createUpdateTombstone = new CreateUpdateTombstone();

  @NotBlank
  private String masExchangeName = "mas-exchange";

  @Data
  @Validated
  public static class Specimen {

    @NotBlank
    private String exchangeName = "digital-specimen-exchange";

    @NotNull
    private String routingKeyName = "digital-specimen";

    @NotBlank
    private String queueName = "digital-specimen-queue";

    @NotBlank
    private String dlqExchangeName = "digital-specimen-exchange-dlq";

    @NotBlank
    private String dlqRoutingKeyName = "digital-specimen-dlq";
  }

  @Data
  @Validated
  public static class DigitalMedia {

    @NotBlank
    private String exchangeName = "digital-media-exchange";

    @NotNull
    private String routingKeyName = "digital-media";
  }

  @Data
  @Validated
  public static class AutoAcceptedAnnotation {

    @NotBlank
    private String exchangeName = "auto-accepted-annotation-exchange";

    @NotNull
    private String routingKeyName = "auto-accepted-annotation";
  }

  @Data
  @Validated
  public static class CreateUpdateTombstone {

    @NotBlank
    private String exchangeName = "create-update-tombstone-exchange";

    @NotNull
    private String routingKeyName = "create-update-tombstone";
  }

}
