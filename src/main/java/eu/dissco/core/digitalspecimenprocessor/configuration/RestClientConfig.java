package eu.dissco.core.digitalspecimenprocessor.configuration;

import static lombok.Lombok.sneakyThrow;

import eu.dissco.core.digitalspecimenprocessor.client.PidClient;
import eu.dissco.core.digitalspecimenprocessor.exception.PidException;
import java.nio.charset.StandardCharsets;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.security.oauth2.client.AuthorizedClientServiceOAuth2AuthorizedClientManager;
import org.springframework.security.oauth2.client.OAuth2AuthorizedClientManager;
import org.springframework.security.oauth2.client.OAuth2AuthorizedClientProviderBuilder;
import org.springframework.security.oauth2.client.OAuth2AuthorizedClientService;
import org.springframework.security.oauth2.client.registration.ClientRegistrationRepository;
import org.springframework.security.oauth2.client.web.client.OAuth2ClientHttpRequestInterceptor;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.support.RestClientAdapter;
import org.springframework.web.service.invoker.HttpServiceProxyFactory;

@Configuration
@Slf4j
public class RestClientConfig {

	@Value("${doi.endpoint}")
	private String doiEndpoint;

	@Bean
	public OAuth2AuthorizedClientManager authorizedClientManager(
			ClientRegistrationRepository clientRegistrationRepository, OAuth2AuthorizedClientService clientService) {
		var authorizedClientProvider = OAuth2AuthorizedClientProviderBuilder.builder()
			.refreshToken()
			.clientCredentials()
			.build();
		var authorizedClientManager = new AuthorizedClientServiceOAuth2AuthorizedClientManager(
				clientRegistrationRepository, clientService);
		authorizedClientManager.setAuthorizedClientProvider(authorizedClientProvider);
		return authorizedClientManager;
	}

	@Bean
	public PidClient doiClient(OAuth2AuthorizedClientManager authorizedClientManager) {
		var interceptor = new OAuth2ClientHttpRequestInterceptor(authorizedClientManager);
		interceptor.setClientRegistrationIdResolver(request -> "dissco");
		// Create RestClient
		var restClient = RestClient.builder()
			.requestInterceptor(interceptor)
			// On status error, log the response and throw a PidException
			.defaultStatusHandler(HttpStatusCode::isError, (request, response) -> {
				var body = new String(response.getBody().readAllBytes(), StandardCharsets.UTF_8);
				log.error("An error has occurred with the DOI service. Status: {}, response: {}",
						response.getStatusCode(), body);
				throw sneakyThrow(new PidException("An error has occurred creating the PID"));
			})
			.baseUrl(doiEndpoint)
			.defaultHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
			.build();
		// Create factory for client proxies
		var proxyFactory = HttpServiceProxyFactory.builder()
			.exchangeAdapter(RestClientAdapter.create(restClient))
			.build();
		// Create client proxy
		return proxyFactory.createClient(PidClient.class);
	}

}
