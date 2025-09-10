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
  @NotNull
  private MasScheduler masScheduler = new MasScheduler();
  @NotNull
  private DigitalMediaRelationshipTombstone digitalMediaRelationshipTombstone = new DigitalMediaRelationshipTombstone();

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

    @NotBlank
    private String queueName = "digital-media-queue";

    @NotBlank
    private String dlqExchangeName = "digital-media-exchange-dlq";

    @NotBlank
    private String dlqRoutingKeyName = "digital-media-dlq";
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
  public static class DigitalMediaRelationshipTombstone {

    @NotBlank
    private String queueName = "digital-media-relationship-tombstone-queue";

    @NotBlank
    private String exchangeName = "digital-media-relationship-tombstone-exchange";

    @NotNull
    private String routingKeyName = "digital-media-relationship-tombstone";

    @NotBlank
    private String dlqExchangeName = "digital-media-relationship-tombstone-exchange-dlq";

    @NotBlank
    private String dlqRoutingKeyName = "digital-media-relationship-tombstone-dlq";
  }

  @Data
  @Validated
  public static class CreateUpdateTombstone {

    @NotBlank
    private String exchangeName = "create-update-tombstone-exchange";

    @NotNull
    private String routingKeyName = "create-update-tombstone";
  }

  @Data
  @Validated
  public static class MasScheduler {

    @NotBlank
    private String exchangeName = "mas-scheduler-exchange";

    @NotNull
    private String routingKeyName = "mas-scheduler";
  }

}
