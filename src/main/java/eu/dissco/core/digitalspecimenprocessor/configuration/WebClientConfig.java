package eu.dissco.core.digitalspecimenprocessor.configuration;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.security.oauth2.client.AuthorizedClientServiceOAuth2AuthorizedClientManager;
import org.springframework.security.oauth2.client.OAuth2AuthorizedClientManager;
import org.springframework.security.oauth2.client.OAuth2AuthorizedClientProviderBuilder;
import org.springframework.security.oauth2.client.OAuth2AuthorizedClientService;
import org.springframework.security.oauth2.client.registration.ClientRegistrationRepository;
import org.springframework.security.oauth2.client.web.reactive.function.client.ServletOAuth2AuthorizedClientExchangeFilterFunction;
import org.springframework.web.reactive.function.client.ExchangeStrategies;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.netty.http.client.HttpClient;

@Configuration
public class WebClientConfig {

  @Value("${handle.endpoint}")
  private String handleEndpoint;

  @Bean
  public OAuth2AuthorizedClientManager authorizedClientManager(
      ClientRegistrationRepository clientRegistrationRepository,
      OAuth2AuthorizedClientService clientService) {
    var authorizedClientProvider = OAuth2AuthorizedClientProviderBuilder
        .builder()
        .refreshToken()
        .clientCredentials()
        .build();
    var authorizedClientManager = new AuthorizedClientServiceOAuth2AuthorizedClientManager(
        clientRegistrationRepository, clientService
    );
    authorizedClientManager.setAuthorizedClientProvider(authorizedClientProvider);
    return authorizedClientManager;
  }

  @Bean
  public WebClient handleClient(OAuth2AuthorizedClientManager authorizedClientManager) {
    var oauth2Client = new ServletOAuth2AuthorizedClientExchangeFilterFunction(
        authorizedClientManager);
    oauth2Client.setDefaultClientRegistrationId("dissco");
    return WebClient.builder()
        .apply(oauth2Client.oauth2Configuration())
        .clientConnector(new ReactorClientHttpConnector(HttpClient.create().followRedirect(true)))
        .baseUrl(handleEndpoint)
        .exchangeStrategies(ExchangeStrategies
            .builder()
            .codecs(codecs -> codecs
                .defaultCodecs()
                .maxInMemorySize(500 * 1024))
            .build())
        .defaultHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
        .build();
  }
}
