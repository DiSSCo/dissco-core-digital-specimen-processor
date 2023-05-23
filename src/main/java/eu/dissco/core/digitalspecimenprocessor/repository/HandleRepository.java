package eu.dissco.core.digitalspecimenprocessor.repository;

import static eu.dissco.core.digitalspecimenprocessor.database.jooq.Tables.HANDLES;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.PRIMARY_SPECIMEN_OBJECT_ID;

import eu.dissco.core.digitalspecimenprocessor.domain.HandleAttribute;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import org.jooq.DSLContext;
import org.jooq.Query;
import org.jooq.Record;
import org.springframework.stereotype.Repository;

@Repository
@RequiredArgsConstructor
public class HandleRepository {

  private final DSLContext context;

  public void createHandle(String handle, Instant recordTimestamp,
      List<HandleAttribute> handleAttributes) {
    var queryList = new ArrayList<Query>();
    for (var handleAttribute : handleAttributes) {
      var query = context.insertInto(HANDLES)
          .set(HANDLES.HANDLE, handle.getBytes(StandardCharsets.UTF_8))
          .set(HANDLES.IDX, handleAttribute.index())
          .set(HANDLES.TYPE, handleAttribute.type().getBytes(StandardCharsets.UTF_8))
          .set(HANDLES.DATA, handleAttribute.data())
          .set(HANDLES.TTL, 86400)
          .set(HANDLES.TIMESTAMP, recordTimestamp.getEpochSecond())
          .set(HANDLES.ADMIN_READ, true)
          .set(HANDLES.ADMIN_WRITE, true)
          .set(HANDLES.PUB_READ, true)
          .set(HANDLES.PUB_WRITE, false);
      queryList.add(query);
    }
    context.batch(queryList).execute();
  }

  public Optional<Record> searchByPrimarySpecimenObjectId(byte[] primarySpecimenObjectId){
    return context.select(HANDLES.asterisk())
        .from(HANDLES)
        .where(HANDLES.TYPE.eq(PRIMARY_SPECIMEN_OBJECT_ID.getAttribute().getBytes(StandardCharsets.UTF_8)))
        .and(HANDLES.DATA.eq(primarySpecimenObjectId))
        .fetchOptional();
  }

  public void updateHandleAttributes(String id, Instant recordTimestamp,
      List<HandleAttribute> handleAttributes, boolean versionIncrement) {
    var queryList = new ArrayList<Query>();
    for (var handleAttribute : handleAttributes) {
      var query = context.update(HANDLES)
          .set(HANDLES.DATA, handleAttribute.data())
          .set(HANDLES.TIMESTAMP, recordTimestamp.getEpochSecond())
          .where(HANDLES.HANDLE.eq(id.getBytes(StandardCharsets.UTF_8)))
          .and(HANDLES.IDX.eq(handleAttribute.index()));
      queryList.add(query);
    }
    queryList.add(versionIncrement(id, recordTimestamp, versionIncrement));
    context.batch(queryList).execute();
  }

  private Query versionIncrement(String pid, Instant recordTimestamp, boolean versionIncrement) {
    var currentVersion =
        Integer.parseInt(context.select(HANDLES.DATA)
            .from(HANDLES)
            .where(HANDLES.HANDLE.eq(pid.getBytes(
                StandardCharsets.UTF_8)))
            .and(HANDLES.TYPE.eq("issueNumber".getBytes(StandardCharsets.UTF_8)))
            .fetchOne(dbRecord -> new String(dbRecord.value1())));
    int version;
    if (versionIncrement){
       version = currentVersion + 1;
    } else {
      version = currentVersion - 1;
    }

    return context.update(HANDLES)
        .set(HANDLES.DATA, String.valueOf(version).getBytes(StandardCharsets.UTF_8))
        .set(HANDLES.TIMESTAMP, recordTimestamp.getEpochSecond())
        .where(HANDLES.HANDLE.eq(pid.getBytes(
            StandardCharsets.UTF_8)))
        .and(HANDLES.TYPE.eq("issueNumber".getBytes(StandardCharsets.UTF_8)));
  }

  public void rollbackHandleCreation(String id) {
    context.delete(HANDLES)
        .where(HANDLES.HANDLE.eq(id.getBytes(StandardCharsets.UTF_8)))
        .execute();
  }

}
