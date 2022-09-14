package eu.dissco.core.digitalspecimenprocessor.configuration;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import eu.dissco.core.digitalspecimenprocessor.property.ElasticSearchProperties;
import lombok.RequiredArgsConstructor;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@RequiredArgsConstructor
public class ElasticSearchConfiguration {

  private final ElasticSearchProperties properties;

  @Bean
  public ElasticsearchClient elasticsearchClient() {
    RestClient restClient = RestClient.builder(new HttpHost(properties.getHostname(),
        properties.getPort())).build();
    ElasticsearchTransport transport = new RestClientTransport(restClient,
        new JacksonJsonpMapper());
    return new ElasticsearchClient(transport);
  }

}
