package eu.dissco.core.digitalspecimenprocessor.repository;

import static eu.dissco.core.digitalspecimenprocessor.database.jooq.Tables.HANDLES;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfile.PRIMARY_SPECIMEN_OBJECT_ID;

import eu.dissco.core.digitalspecimenprocessor.domain.HandleAttribute;
import eu.dissco.core.digitalspecimenprocessor.domain.IdentifierTuple;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.jooq.DSLContext;
import org.jooq.Query;
import org.jooq.Record;
import org.springframework.stereotype.Repository;

@Repository
@RequiredArgsConstructor
public class HandleRepository {

  private final DSLContext context;
  private static final int TTL = 86400;

  public void createHandle(Instant recordTimestamp,
      List<HandleAttribute> handleAttributes) {
    var queryList = new ArrayList<Query>();
    for (var handleAttribute : handleAttributes) {
      var query = context.insertInto(HANDLES)
          .set(HANDLES.HANDLE, handleAttribute.handle().getBytes(StandardCharsets.UTF_8))
          .set(HANDLES.IDX, handleAttribute.index())
          .set(HANDLES.TYPE, handleAttribute.type().getBytes(StandardCharsets.UTF_8))
          .set(HANDLES.DATA, handleAttribute.data())
          .set(HANDLES.TTL, TTL)
          .set(HANDLES.TIMESTAMP, recordTimestamp.getEpochSecond())
          .set(HANDLES.ADMIN_READ, true)
          .set(HANDLES.ADMIN_WRITE, true)
          .set(HANDLES.PUB_READ, true)
          .set(HANDLES.PUB_WRITE, false);
      queryList.add(query);
    }
    context.batch(queryList).execute();
  }

  public List<IdentifierTuple> searchByPrimarySpecimenObjectId(List<byte[]> primarySpecimenObjectId) {
    return context.select(HANDLES.asterisk())
        .from(HANDLES)
        .where(HANDLES.TYPE.eq(
            PRIMARY_SPECIMEN_OBJECT_ID.getAttribute().getBytes(StandardCharsets.UTF_8)))
        .and(HANDLES.DATA.in(primarySpecimenObjectId))
        .fetch(this::mapToIdentifierTuple);
  }

  private IdentifierTuple mapToIdentifierTuple(Record dbRecord){
    return new IdentifierTuple(new String(dbRecord.get(HANDLES.HANDLE), StandardCharsets.UTF_8), new String(dbRecord.get(HANDLES.DATA), StandardCharsets.UTF_8));
  }

  public void updateHandleAttributesBatch(Instant recordTimestamp, List<List<HandleAttribute>> handleRecords){
    List<Query> queryList = new ArrayList<>();
    for (List<HandleAttribute> handleRecord : handleRecords) {
      queryList.addAll(
          prepareUpsertQuery(handleRecord.get(0).handle(), recordTimestamp, handleRecord, true));
    }
    context.batch(queryList).execute();
  }

  public void updateHandleAttributes(String id, Instant recordTimestamp,
      List<HandleAttribute> handleAttributes, boolean versionIncrement) {
    var queryList = prepareUpsertQuery(id, recordTimestamp, handleAttributes, versionIncrement);
    context.batch(queryList).execute();
  }

  private ArrayList<Query> prepareUpsertQuery(String id, Instant recordTimestamp,
      List<HandleAttribute> handleAttributes, boolean versionIncrement) {
    var queryList = new ArrayList<Query>();
    for (var handleAttribute : handleAttributes) {
      var query = context.insertInto(HANDLES)
          .set(HANDLES.HANDLE, handleAttribute.handle().getBytes(StandardCharsets.UTF_8))
          .set(HANDLES.IDX, handleAttribute.index())
          .set(HANDLES.TYPE, handleAttribute.type().getBytes(StandardCharsets.UTF_8))
          .set(HANDLES.DATA, handleAttribute.data())
          .set(HANDLES.TTL, TTL)
          .set(HANDLES.TIMESTAMP, recordTimestamp.getEpochSecond())
          .set(HANDLES.ADMIN_READ, true)
          .set(HANDLES.ADMIN_WRITE, true)
          .set(HANDLES.PUB_READ, true)
          .set(HANDLES.PUB_WRITE, false)
          .onDuplicateKeyUpdate()
          .set(HANDLES.HANDLE, handleAttribute.handle().getBytes(StandardCharsets.UTF_8))
          .set(HANDLES.IDX, handleAttribute.index())
          .set(HANDLES.TYPE, handleAttribute.type().getBytes(StandardCharsets.UTF_8))
          .set(HANDLES.DATA, handleAttribute.data())
          .set(HANDLES.TTL, TTL)
          .set(HANDLES.TIMESTAMP, recordTimestamp.getEpochSecond())
          .set(HANDLES.ADMIN_READ, true)
          .set(HANDLES.ADMIN_WRITE, true)
          .set(HANDLES.PUB_READ, true)
          .set(HANDLES.PUB_WRITE, false);
      queryList.add(query);
    }
    // If we move this query elsewhere, we won't need "id" as an argument
    // Batching would be a bit simpler, i.e. List<attributes> instead of List<List<attributes>>
    queryList.add(versionIncrement(id, recordTimestamp, versionIncrement));
    return queryList;
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
    if (versionIncrement) {
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
