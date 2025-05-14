package eu.dissco.core.digitalspecimenprocessor.repository;

import static eu.dissco.core.digitalspecimenprocessor.util.DigitalObjectUtils.flattenToDigitalMedia;
import static eu.dissco.core.digitalspecimenprocessor.util.DigitalObjectUtils.flattenToDigitalSpecimen;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.DOI_PREFIX;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.HANDLE;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MAPPER;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MEDIA_PID;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalMediaRecord;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenRecord;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenUnequalDigitalMediaRecord;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenUnequalDigitalSpecimenRecord;
import static org.assertj.core.api.Assertions.assertThat;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import eu.dissco.core.digitalspecimenprocessor.property.ElasticSearchProperties;
import eu.dissco.core.digitalspecimenprocessor.schema.DigitalMedia;
import eu.dissco.core.digitalspecimenprocessor.schema.DigitalSpecimen;
import java.io.IOException;
import java.util.Set;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
class ElasticSearchRepositoryIT {

  private static final DockerImageName ELASTIC_IMAGE = DockerImageName.parse(
      "docker.elastic.co/elasticsearch/elasticsearch").withTag("8.6.1");
  private static final String INDEX = "digital-object";
  private static final String ELASTICSEARCH_USERNAME = "elastic";
  private static final String ELASTICSEARCH_PASSWORD = "s3cret";
  private static final ElasticsearchContainer container = new ElasticsearchContainer(
      ELASTIC_IMAGE).withPassword(ELASTICSEARCH_PASSWORD);
  private static ElasticsearchClient client;
  private static RestClient restClient;
  private final ElasticSearchProperties esProperties = new ElasticSearchProperties();
  private ElasticSearchRepository repository;

  @BeforeAll
  static void initContainer() {
    // Create the elasticsearch container.
    container.start();

    final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
    credentialsProvider.setCredentials(AuthScope.ANY,
        new UsernamePasswordCredentials(ELASTICSEARCH_USERNAME, ELASTICSEARCH_PASSWORD));

    HttpHost host = new HttpHost("localhost",
        container.getMappedPort(9200), "https");
    final RestClientBuilder builder = RestClient.builder(host);

    builder.setHttpClientConfigCallback(clientBuilder -> {
      clientBuilder.setSSLContext(container.createSslContextFromCa());
      clientBuilder.setDefaultCredentialsProvider(credentialsProvider);
      return clientBuilder;
    });
    restClient = builder.build();

    ElasticsearchTransport transport = new RestClientTransport(restClient,
        new JacksonJsonpMapper(MAPPER));

    client = new ElasticsearchClient(transport);
  }

  @AfterAll
  static void closeResources() throws Exception {
    restClient.close();
  }

  @BeforeEach
  void initRepository() {
    esProperties.setMediaIndexName(INDEX);
    esProperties.setSpecimenIndexName(INDEX);
    repository = new ElasticSearchRepository(client, esProperties);
  }

  @AfterEach
  void clearIndex() throws IOException {
      client.indices().delete(b -> b.index(INDEX));

  }

  @Test
  void testIndexDigitalMedia() throws IOException {
    // Given
    var expected = flattenToDigitalMedia(givenDigitalMediaRecord());

    // When
    var result = repository.indexDigitalMedia(Set.of(
        givenDigitalMediaRecord()));
    var document = client.get(g -> g.index(INDEX).id(DOI_PREFIX + MEDIA_PID),
        DigitalMedia.class);

    // Then
    assertThat(result.errors()).isFalse();
    assertThat(document.source()).isEqualTo(expected);
    assertThat(result.items().get(0).result()).isEqualTo("created");
  }

  @Test
  void testIndexDigitalSpecimen() throws IOException {
    // Given
    var expected = flattenToDigitalSpecimen(givenDigitalSpecimenRecord());

    // When
    var result = repository.indexDigitalSpecimen(Set.of(
        givenDigitalSpecimenRecord()));
    var document = client.get(g -> g.index(INDEX).id(DOI_PREFIX + HANDLE),
        DigitalSpecimen.class);

    // Then
    assertThat(result.errors()).isFalse();
    assertThat(document.source()).isEqualTo(expected);
    assertThat(result.items().get(0).result()).isEqualTo("created");
  }

  @Test
  void testRollbackSpecimen() throws IOException {
    // Given
    repository.indexDigitalSpecimen(Set.of(givenDigitalSpecimenRecord()));

    // When
    repository.rollbackObject(HANDLE, true);
    var document =  client.get(g -> g.index(INDEX).id(DOI_PREFIX + HANDLE),
        DigitalSpecimen.class);

    // Then
    assertThat(document.found()).isFalse();
  }

  @Test
  void testRollbackMedia() throws IOException {
    // Given
    repository.indexDigitalMedia(Set.of(givenDigitalMediaRecord()));

    // When
    repository.rollbackObject(MEDIA_PID, false);
    var document =  client.get(g -> g.index(INDEX).id(DOI_PREFIX + MEDIA_PID),
        DigitalMedia.class);

    // Then
    assertThat(document.found()).isFalse();
  }

  @Test
  void testRollbackVersionSpecimen() throws IOException {
    // Given
    repository.indexDigitalSpecimen(Set.of(givenUnequalDigitalSpecimenRecord()));
    var expected = flattenToDigitalSpecimen(givenDigitalSpecimenRecord());

    // When
    repository.rollbackVersion(givenDigitalSpecimenRecord());
    var document =  client.get(g -> g.index(INDEX).id(DOI_PREFIX + HANDLE),
        DigitalSpecimen.class);

    // Then
    assertThat(document.source()).isEqualTo(expected);
  }

  @Test
  void testRollbackVersionMedia() throws IOException {
    // Given
    repository.indexDigitalMedia(Set.of(givenUnequalDigitalMediaRecord()));
    var expected = flattenToDigitalMedia(givenDigitalMediaRecord());

    // When
    repository.rollbackVersion(givenDigitalMediaRecord());
    var document =  client.get(g -> g.index(INDEX).id(DOI_PREFIX + MEDIA_PID),
        DigitalMedia.class);

    // Then
    assertThat(document.source()).isEqualTo(expected);
  }

}
