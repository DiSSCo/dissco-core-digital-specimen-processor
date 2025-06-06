package eu.dissco.core.digitalspecimenprocessor.domain.media;

import java.util.List;

public record MediaProcessResult(
    List<DigitalMediaRecord> equalDigitalMedia,
    List<UpdatedDigitalMediaTuple> changedDigitalMedia,
    List<DigitalMediaEvent> newDigitalMedia
) {

}
