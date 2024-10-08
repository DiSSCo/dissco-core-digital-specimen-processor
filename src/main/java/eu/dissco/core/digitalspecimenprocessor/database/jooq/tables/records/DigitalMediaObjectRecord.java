/*
 * This file is generated by jOOQ.
 */
package eu.dissco.core.digitalspecimenprocessor.database.jooq.tables.records;


import eu.dissco.core.digitalspecimenprocessor.database.jooq.tables.DigitalMediaObject;
import java.time.Instant;
import org.jooq.JSONB;
import org.jooq.Record1;
import org.jooq.impl.UpdatableRecordImpl;


/**
 * This class is generated by jOOQ.
 */
@SuppressWarnings({"all", "unchecked", "rawtypes"})
public class DigitalMediaObjectRecord extends UpdatableRecordImpl<DigitalMediaObjectRecord> {

  private static final long serialVersionUID = 1L;

  /**
   * Setter for <code>public.digital_media_object.id</code>.
   */
  public void setId(String value) {
    set(0, value);
  }

  /**
   * Getter for <code>public.digital_media_object.id</code>.
   */
  public String getId() {
    return (String) get(0);
  }

  /**
   * Setter for <code>public.digital_media_object.version</code>.
   */
  public void setVersion(Integer value) {
    set(1, value);
  }

  /**
   * Getter for <code>public.digital_media_object.version</code>.
   */
  public Integer getVersion() {
    return (Integer) get(1);
  }

  /**
   * Setter for <code>public.digital_media_object.type</code>.
   */
  public void setType(String value) {
    set(2, value);
  }

  /**
   * Getter for <code>public.digital_media_object.type</code>.
   */
  public String getType() {
    return (String) get(2);
  }

  /**
   * Setter for <code>public.digital_media_object.digital_specimen_id</code>.
   */
  public void setDigitalSpecimenId(String value) {
    set(3, value);
  }

  /**
   * Getter for <code>public.digital_media_object.digital_specimen_id</code>.
   */
  public String getDigitalSpecimenId() {
    return (String) get(3);
  }

  /**
   * Setter for <code>public.digital_media_object.media_url</code>.
   */
  public void setMediaUrl(String value) {
    set(4, value);
  }

  /**
   * Getter for <code>public.digital_media_object.media_url</code>.
   */
  public String getMediaUrl() {
    return (String) get(4);
  }

  /**
   * Setter for <code>public.digital_media_object.created</code>.
   */
  public void setCreated(Instant value) {
    set(5, value);
  }

  /**
   * Getter for <code>public.digital_media_object.created</code>.
   */
  public Instant getCreated() {
    return (Instant) get(5);
  }

  /**
   * Setter for <code>public.digital_media_object.last_checked</code>.
   */
  public void setLastChecked(Instant value) {
    set(6, value);
  }

  /**
   * Getter for <code>public.digital_media_object.last_checked</code>.
   */
  public Instant getLastChecked() {
    return (Instant) get(6);
  }

  /**
   * Setter for <code>public.digital_media_object.deleted</code>.
   */
  public void setDeleted(Instant value) {
    set(7, value);
  }

  /**
   * Getter for <code>public.digital_media_object.deleted</code>.
   */
  public Instant getDeleted() {
    return (Instant) get(7);
  }

  /**
   * Setter for <code>public.digital_media_object.data</code>.
   */
  public void setData(JSONB value) {
    set(8, value);
  }

  /**
   * Getter for <code>public.digital_media_object.data</code>.
   */
  public JSONB getData() {
    return (JSONB) get(8);
  }

  /**
   * Setter for <code>public.digital_media_object.original_data</code>.
   */
  public void setOriginalData(JSONB value) {
    set(9, value);
  }

  /**
   * Getter for <code>public.digital_media_object.original_data</code>.
   */
  public JSONB getOriginalData() {
    return (JSONB) get(9);
  }

  // -------------------------------------------------------------------------
  // Primary key information
  // -------------------------------------------------------------------------

  @Override
  public Record1<String> key() {
    return (Record1) super.key();
  }

  // -------------------------------------------------------------------------
  // Constructors
  // -------------------------------------------------------------------------

  /**
   * Create a detached DigitalMediaObjectRecord
   */
  public DigitalMediaObjectRecord() {
    super(DigitalMediaObject.DIGITAL_MEDIA_OBJECT);
  }

  /**
   * Create a detached, initialised DigitalMediaObjectRecord
   */
  public DigitalMediaObjectRecord(String id, Integer version, String type, String digitalSpecimenId,
      String mediaUrl, Instant created, Instant lastChecked, Instant deleted, JSONB data,
      JSONB originalData) {
    super(DigitalMediaObject.DIGITAL_MEDIA_OBJECT);

    setId(id);
    setVersion(version);
    setType(type);
    setDigitalSpecimenId(digitalSpecimenId);
    setMediaUrl(mediaUrl);
    setCreated(created);
    setLastChecked(lastChecked);
    setDeleted(deleted);
    setData(data);
    setOriginalData(originalData);
    resetChangedOnNotNull();
  }
}