package eu.dissco.core.digitalspecimenprocessor.repository;

import static eu.dissco.core.digitalspecimenprocessor.database.jooq.Tables.DIGITAL_MEDIA_OBJECT;

import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaKey;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.springframework.stereotype.Repository;

@Repository
@RequiredArgsConstructor
public class DigitalMediaRepository {

  private final DSLContext context;

  public Map<DigitalMediaKey, String> getDigitalMediaUrisFromId(List<String> mediaPids) {
    return context.select(DIGITAL_MEDIA_OBJECT.DIGITAL_SPECIMEN_ID, DIGITAL_MEDIA_OBJECT.MEDIA_URL,
            DIGITAL_MEDIA_OBJECT.ID)
        .from(DIGITAL_MEDIA_OBJECT)
        .where(DIGITAL_MEDIA_OBJECT.ID.in(mediaPids))
        .fetch()
        .stream()
        .collect(Collectors.toMap(
            DigitalMediaRepository::toDigitalMediaKey,
            r -> r.get(DIGITAL_MEDIA_OBJECT.ID)
        ));
  }

  public void removeSpecimenRelationshipsFromMedia(List<String> mediaPids) {
    context.update(DIGITAL_MEDIA_OBJECT)
        .set(DIGITAL_MEDIA_OBJECT.DIGITAL_SPECIMEN_ID, "UNKNOWN")
        .where(DIGITAL_MEDIA_OBJECT.ID.in(mediaPids))
        .execute();
  }

  private static DigitalMediaKey toDigitalMediaKey(Record dbRecord) {
    return new DigitalMediaKey(dbRecord.get(DIGITAL_MEDIA_OBJECT.DIGITAL_SPECIMEN_ID),
        dbRecord.get(DIGITAL_MEDIA_OBJECT.MEDIA_URL));
  }

}
