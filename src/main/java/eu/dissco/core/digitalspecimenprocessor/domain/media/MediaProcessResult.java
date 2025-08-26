package eu.dissco.core.digitalspecimenprocessor.domain.media;

import java.util.List;

public record MediaProcessResult(
    List<DigitalMediaRecord> equalMedia,
    List<DigitalMediaRecord> updatedMedia,
    List<DigitalMediaRecord> newMedia
) {

}
