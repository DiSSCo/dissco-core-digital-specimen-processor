package eu.dissco.core.digitalspecimenprocessor.configuration;

import com.fasterxml.jackson.core.JsonFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class JsonConfiguration {

  @Bean
  public JsonFactory jsonFactory() {
    return new JsonFactory();
  }

}
