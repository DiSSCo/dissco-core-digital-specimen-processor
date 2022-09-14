package eu.dissco.core.digitalspecimenprocessor.repository;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import eu.dissco.core.digitalspecimenprocessor.domain.DigitalSpecimenRecord;
import eu.dissco.core.digitalspecimenprocessor.property.ElasticSearchProperties;
import java.io.IOException;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Repository;

@Repository
@RequiredArgsConstructor
public class ElasticSearchRepository {

  private final ElasticsearchClient client;
  private final ElasticSearchProperties properties;

  public IndexResponse indexDigitalSpecimen(DigitalSpecimenRecord digitalSpecimen) {
    try {
      return client.index(idx -> idx.index(properties.getIndexName()).id(digitalSpecimen.id()).document(digitalSpecimen));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
