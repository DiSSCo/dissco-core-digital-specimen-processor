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
import co.elastic.clients.transport.rest5_client.Rest5ClientTransport;
import co.elastic.clients.transport.rest5_client.low_level.Rest5Client;
import eu.dissco.core.digitalspecimenprocessor.property.ElasticSearchProperties;
import eu.dissco.core.digitalspecimenprocessor.schema.DigitalMedia;
import eu.dissco.core.digitalspecimenprocessor.schema.DigitalSpecimen;
import java.io.IOException;
import java.util.Base64;
import java.util.Set;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.message.BasicHeader;
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
      "docker.elastic.co/elasticsearch/elasticsearch").withTag("9.2.0");
  private static final String INDEX = "digital-object";
  private static final String ELASTICSEARCH_USERNAME = "elastic";
  private static final String ELASTICSEARCH_PASSWORD = "s3cret";
  private static final ElasticsearchContainer container = new ElasticsearchContainer(
      ELASTIC_IMAGE).withPassword(ELASTICSEARCH_PASSWORD);
  private static ElasticsearchClient client;
  private static Rest5Client restClient;
  private final ElasticSearchProperties esProperties = new ElasticSearchProperties();
  private ElasticSearchRepository repository;

  @BeforeAll
  static void initContainer() throws InterruptedException {
    // Create the elasticsearch container.
    container.start();
    Thread.sleep(2500);
    var creds = Base64.getEncoder()
        .encodeToString((ELASTICSEARCH_USERNAME + ":" + ELASTICSEARCH_PASSWORD).getBytes());

    restClient = Rest5Client.builder(
            new HttpHost("https", "localhost", container.getMappedPort(9200)))
        .setDefaultHeaders(new Header[]{new BasicHeader("Authorization", "Basic " + creds)})
        .setSSLContext(container.createSslContextFromCa()).build();

    ElasticsearchTransport transport = new Rest5ClientTransport(restClient,
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
    var document = client.get(g -> g.index(INDEX).id(DOI_PREFIX + HANDLE),
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
    var document = client.get(g -> g.index(INDEX).id(DOI_PREFIX + MEDIA_PID),
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
    var document = client.get(g -> g.index(INDEX).id(DOI_PREFIX + HANDLE),
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
    var document = client.get(g -> g.index(INDEX).id(DOI_PREFIX + MEDIA_PID),
        DigitalMedia.class);

    // Then
    assertThat(document.source()).isEqualTo(expected);
  }

}
