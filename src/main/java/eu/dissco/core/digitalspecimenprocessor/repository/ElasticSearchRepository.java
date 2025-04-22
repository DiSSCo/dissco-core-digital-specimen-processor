package eu.dissco.core.digitalspecimenprocessor.repository;

import static eu.dissco.core.digitalspecimenprocessor.util.DigitalObjectUtils.DOI_PREFIX;
import static eu.dissco.core.digitalspecimenprocessor.util.DigitalObjectUtils.flattenToDigitalSpecimen;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.DeleteResponse;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaRecord;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenRecord;
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

  public BulkResponse indexDigitalSpecimen(Collection<DigitalSpecimenRecord> digitalSpecimenRecords)
      throws IOException {
    var bulkRequest = new BulkRequest.Builder();
    for (var digitalSpecimenrecord : digitalSpecimenRecords) {
      var digitalSpecimen = flattenToDigitalSpecimen(digitalSpecimenrecord);
      bulkRequest.operations(op ->
          op.index(idx -> idx
              .index(properties.getIndexName())
              .id(digitalSpecimen.getId())
              .document(digitalSpecimen))
      );
    }
    return client.bulk(bulkRequest.build());
  }

  public DeleteResponse rollbackSpecimen(String id)
      throws IOException {
    return client.delete(
        d -> d.index(properties.getIndexName()).id(DOI_PREFIX + id));
  }

  public void rollbackVersion(DigitalSpecimenRecord currentDigitalSpecimen) throws IOException {
    var digitalSpecimen = flattenToDigitalSpecimen(currentDigitalSpecimen);
    client.index(i -> i.index(properties.getIndexName()).id(digitalSpecimen.getId())
        .document(digitalSpecimen));
  }

  public BulkResponse indexDigitalMedia(
      Collection<DigitalMediaRecord> digitalMediaRecords) throws IOException {
    var bulkRequest = new BulkRequest.Builder();
    for (var digitalMediaRecord : digitalMediaRecords) {
      var digitalMedia = digitalMediaRecord.attributes();
      bulkRequest.operations(op ->
          op.index(idx ->
              idx.index(properties.getIndexName())
                  .id(digitalMedia.getId())
                  .document(digitalMedia))
      );
    }
    return client.bulk(bulkRequest.build());
  }

  public DeleteResponse rollbackDigitalMedia(DigitalMediaRecord digitalMediaRecord)
      throws IOException {
    return client.delete(
        d -> d.index(properties.getIndexName()).id(DOI_PREFIX + digitalMediaRecord.id()));
  }

  public void rollbackVersion(DigitalMediaRecord currentDigitalMediaRecord)
      throws IOException {
    var digitalMedia = currentDigitalMediaRecord.attributes();
    client.index(i -> i.index(properties.getIndexName()).id(digitalMedia.getId())
        .document(digitalMedia));
  }

}
