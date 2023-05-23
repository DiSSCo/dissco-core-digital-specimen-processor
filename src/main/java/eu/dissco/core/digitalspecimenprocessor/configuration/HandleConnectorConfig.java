package eu.dissco.core.digitalspecimenprocessor.configuration;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.netty.http.client.HttpClient;

@Configuration
public class HandleConnectorConfig {
  @Bean
  public WebClient handleResolverClient() {
    return WebClient.builder()
        .clientConnector(new ReactorClientHttpConnector(HttpClient.create()))
        .baseUrl("sandbox.dissco.tech/handle-manager/api")
        .build();
  }
}
