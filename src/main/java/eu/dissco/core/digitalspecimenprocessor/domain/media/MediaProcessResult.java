package eu.dissco.core.digitalspecimenprocessor.domain.media;

import java.util.List;
import java.util.Map;

public record MediaProcessResult(
    List<DigitalMediaRecord> equalDigitalMedia,
    List<UpdatedDigitalMediaTuple> changedDigitalMedia,
    List<DigitalMediaEventWithoutDOI> newDigitalMedia,
    Map<String, String> newMediaPids) {

}
