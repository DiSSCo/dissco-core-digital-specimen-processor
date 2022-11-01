package eu.dissco.core.digitalspecimenprocessor.repository;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import eu.dissco.core.digitalspecimenprocessor.domain.DigitalSpecimenRecord;
import eu.dissco.core.digitalspecimenprocessor.property.ElasticSearchProperties;
import java.io.IOException;
import java.util.Collection;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Repository;

@Repository
@RequiredArgsConstructor
public class ElasticSearchRepository {

  private final ElasticsearchClient client;
  private final ElasticSearchProperties properties;

  public BulkResponse indexDigitalSpecimen(Collection<DigitalSpecimenRecord> digitalSpecimens)
      throws IOException {
    var bulkRequest = new BulkRequest.Builder();
    for (var digitalSpecimen : digitalSpecimens) {
      bulkRequest.operations(op ->
          op.index(idx -> idx
              .index(properties.getIndexName())
              .id(digitalSpecimen.id())
              .document(digitalSpecimen))
      );
    }
    return client.bulk(bulkRequest.build());
  }
}
