package eu.dissco.core.digitalspecimenprocessor.configuration;

import com.jayway.jsonpath.Option;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class JsonPathConfiguration {

  @Bean
  public com.jayway.jsonpath.Configuration jsonPathConfiguration() {
    return com.jayway.jsonpath.Configuration.builder()
        .options(Option.AS_PATH_LIST, Option.SUPPRESS_EXCEPTIONS, Option.ALWAYS_RETURN_LIST)
        .build();
  }

}
