package eu.dissco.core.digitalspecimenprocessor.repository;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.DeleteResponse;
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

  public DeleteResponse rollbackSpecimen(DigitalSpecimenRecord digitalSpecimenRecord)
      throws IOException {
    return client.delete(d -> d.index(properties.getIndexName()).id(digitalSpecimenRecord.id()));
  }

  public void rollbackVersion(DigitalSpecimenRecord currentDigitalSpecimen) throws IOException {
    client.index(i -> i.index(properties.getIndexName()).id(currentDigitalSpecimen.id())
        .document(currentDigitalSpecimen));
  }
}
