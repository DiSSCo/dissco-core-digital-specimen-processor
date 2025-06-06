package eu.dissco.core.digitalspecimenprocessor.repository;

import static eu.dissco.core.digitalspecimenprocessor.util.DigitalObjectUtils.DOI_PREFIX;
import static eu.dissco.core.digitalspecimenprocessor.util.DigitalObjectUtils.flattenToDigitalMedia;
import static eu.dissco.core.digitalspecimenprocessor.util.DigitalObjectUtils.flattenToDigitalSpecimen;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.DeleteResponse;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaRecord;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenRecord;
import eu.dissco.core.digitalspecimenprocessor.property.ElasticSearchProperties;
import java.io.IOException;
import java.util.Set;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Repository;

@Repository
@RequiredArgsConstructor
public class ElasticSearchRepository {

  private final ElasticsearchClient client;
  private final ElasticSearchProperties properties;

  public BulkResponse indexDigitalSpecimen(Set<DigitalSpecimenRecord> digitalSpecimenRecords)
      throws IOException {
    var bulkRequest = new BulkRequest.Builder();
    for (var digitalSpecimenrecord : digitalSpecimenRecords) {
      var digitalSpecimen = flattenToDigitalSpecimen(digitalSpecimenrecord);
      bulkRequest.operations(op ->
          op.index(idx -> idx
              .index(properties.getSpecimenIndexName())
              .id(digitalSpecimen.getId())
              .document(digitalSpecimen))
      );
    }
    return client.bulk(bulkRequest.build());
  }

  public DeleteResponse rollbackObject(String id, boolean isSpecimen)
      throws IOException {
    var index = isSpecimen ? properties.getSpecimenIndexName() : properties.getMediaIndexName();
    return client.delete(
        d -> d.index(index).id(DOI_PREFIX + id));
  }

  public void rollbackVersion(DigitalSpecimenRecord currentDigitalSpecimen) throws IOException {
    var digitalSpecimen = flattenToDigitalSpecimen(currentDigitalSpecimen);
    client.index(i -> i.index(properties.getSpecimenIndexName()).id(digitalSpecimen.getId())
        .document(digitalSpecimen));
  }

  public void rollbackVersion(DigitalMediaRecord currentDigitalMediaRecord)
      throws IOException {
    var digitalMedia = flattenToDigitalMedia(currentDigitalMediaRecord);
    client.index(i -> i.index(properties.getMediaIndexName()).id(digitalMedia.getId())
        .document(digitalMedia));
  }

  public BulkResponse indexDigitalMedia(
      Set<DigitalMediaRecord> digitalMediaRecords) throws IOException {
    var bulkRequest = new BulkRequest.Builder();
    for (var digitalMediaRecord : digitalMediaRecords) {
      var digitalMedia = flattenToDigitalMedia(digitalMediaRecord);
      bulkRequest.operations(op ->
          op.index(idx ->
              idx.index(properties.getMediaIndexName())
                  .id(digitalMedia.getId())
                  .document(digitalMedia))
      );
    }
    return client.bulk(bulkRequest.build());
  }
}
