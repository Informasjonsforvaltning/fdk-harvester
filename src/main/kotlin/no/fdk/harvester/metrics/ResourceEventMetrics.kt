package no.fdk.harvester.metrics

import io.micrometer.core.instrument.Metrics
import no.fdk.harvest.DataType

object ResourceEventMetrics {
    fun recordPublish(
        dataType: DataType,
        kind: ResourceEventKind,
        success: Boolean,
    ) {
        Metrics
            .counter(
                "resource_event_publish_total",
                "status",
                if (success) "success" else "error",
                "type",
                HarvestMetrics.metricType(dataType),
                "kind",
                kind.label,
            ).increment()
    }

    enum class ResourceEventKind(
        val label: String,
    ) {
        HARVESTED("harvested"),
        REMOVED("removed"),
    }
}
