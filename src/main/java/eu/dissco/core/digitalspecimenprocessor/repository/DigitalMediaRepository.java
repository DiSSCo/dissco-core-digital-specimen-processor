package eu.dissco.core.digitalspecimenprocessor.repository;

import static eu.dissco.core.digitalspecimenprocessor.database.jooq.Tables.DIGITAL_MEDIA_OBJECT;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaRecord;
import eu.dissco.core.digitalspecimenprocessor.schema.DigitalMedia;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jooq.DSLContext;
import org.jooq.JSONB;
import org.jooq.Query;
import org.jooq.Record;
import org.springframework.stereotype.Repository;

@Repository
@RequiredArgsConstructor
@Slf4j
public class DigitalMediaRepository {

  private final DSLContext context;
  private final ObjectMapper mapper;

  // Maps Media URI to its DOI
  public List<DigitalMediaRecord> getExistingDigitalMedia(Set<String> mediaURIs) {
    return context.select(DIGITAL_MEDIA_OBJECT.asterisk())
        .from(DIGITAL_MEDIA_OBJECT)
        .where(DIGITAL_MEDIA_OBJECT.MEDIA_URL.in(mediaURIs))
        .fetch(this::mapDigitalMedia);
  }


  public void removeSpecimenRelationshipsFromMedia(List<String> mediaPids) {
    context.update(DIGITAL_MEDIA_OBJECT)
        .set(DIGITAL_MEDIA_OBJECT.DIGITAL_SPECIMEN_ID, "UNKNOWN")
        .where(DIGITAL_MEDIA_OBJECT.ID.in(mediaPids))
        .execute();
  }


  private DigitalMediaRecord mapDigitalMedia(Record dbRecord) {
    try {
      return new DigitalMediaRecord(
          dbRecord.get(DIGITAL_MEDIA_OBJECT.ID),
          dbRecord.get(DIGITAL_MEDIA_OBJECT.MEDIA_URL),
          List.of(), mapper.readValue(dbRecord.get(DIGITAL_MEDIA_OBJECT.DATA).data(), DigitalMedia.class));
    } catch (JsonProcessingException e) {
      log.error("Unable to map record data to json: {}", dbRecord);
      return null;
    }
  }

  public void updateLastChecked(List<String> currentDigitalMedia) {
    context.update(DIGITAL_MEDIA_OBJECT)
        .set(DIGITAL_MEDIA_OBJECT.LAST_CHECKED, Instant.now())
        .where(DIGITAL_MEDIA_OBJECT.ID.in(currentDigitalMedia))
        .execute();
  }

  public int[] createDigitalMediaRecord(
      Collection<DigitalMediaRecord> digitalMediaRecords) {
    var queries = digitalMediaRecords.stream().map(this::digitalMediaToQuery).toList();
    return context.batch(queries).execute();
  }

  public Query digitalMediaToQuery(DigitalMediaRecord digitalMediaRecord) {
    return context.insertInto(DIGITAL_MEDIA_OBJECT)
        .set(DIGITAL_MEDIA_OBJECT.ID, digitalMediaRecord.id())
        .set(DIGITAL_MEDIA_OBJECT.TYPE, digitalMediaRecord.attributes().getOdsFdoType())
        .set(DIGITAL_MEDIA_OBJECT.VERSION, digitalMediaRecord.attributes().getOdsVersion())
        .set(DIGITAL_MEDIA_OBJECT.MEDIA_URL,
            digitalMediaRecord.attributes().getAcAccessURI())
        .set(DIGITAL_MEDIA_OBJECT.CREATED, digitalMediaRecord.attributes().getDctermsCreated().toInstant())
        .set(DIGITAL_MEDIA_OBJECT.LAST_CHECKED, Instant.now())
        .set(DIGITAL_MEDIA_OBJECT.DATA,
            JSONB.jsonb(
                mapper.valueToTree(digitalMediaRecord.attributes())
                    .toString()))
        // todo - do we keep original data?
        .onConflict(DIGITAL_MEDIA_OBJECT.ID).doUpdate()
        .set(DIGITAL_MEDIA_OBJECT.TYPE, digitalMediaRecord.attributes().getOdsFdoType())
        .set(DIGITAL_MEDIA_OBJECT.VERSION, digitalMediaRecord.attributes().getOdsVersion())
        .set(DIGITAL_MEDIA_OBJECT.MEDIA_URL,
            digitalMediaRecord.attributes().getAcAccessURI())
        .set(DIGITAL_MEDIA_OBJECT.CREATED, digitalMediaRecord.attributes().getDctermsCreated().toInstant())
        .set(DIGITAL_MEDIA_OBJECT.LAST_CHECKED, Instant.now())
        .set(DIGITAL_MEDIA_OBJECT.DATA,
            JSONB.jsonb(
                mapper.valueToTree(digitalMediaRecord.attributes())
                    .toString()));
  }

}
