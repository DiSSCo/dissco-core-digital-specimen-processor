package eu.dissco.core.digitalspecimenprocessor.configuration;

import static eu.dissco.core.digitalspecimenprocessor.configuration.ApplicationConfiguration.DATE_STRING;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class InstantSerializer extends JsonSerializer<Instant> {

  private final DateTimeFormatter formatter = DateTimeFormatter.ofPattern(DATE_STRING).withZone(
      ZoneOffset.UTC);

  @Override
  public void serialize(Instant value, JsonGenerator jsonGenerator,
      SerializerProvider serializerProvider) {
    try {
      jsonGenerator.writeString(formatter.format(value));
    } catch (IOException e) {
      log.error("An error has occurred serializing a date. More information: {}", e.getMessage());
    }
  }
}
