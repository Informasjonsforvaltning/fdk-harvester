package no.fdk.harvester.metrics

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Metrics
import no.fdk.harvest.DataType

object ResourceEventMetrics {
    private var registry: MeterRegistry = Metrics.globalRegistry

    fun bind(registry: MeterRegistry) {
        this.registry = registry
    }

    fun recordPublish(
        dataType: DataType,
        kind: ResourceEventKind,
        outcome: PublishOutcome,
    ) {
        registry
            .counter(
                "resource_event_publish_total",
                "status",
                outcome.status,
                "reason",
                outcome.reason,
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

    enum class PublishOutcome(
        val status: String,
        val reason: String,
    ) {
        SUCCESS("success", "published"),
        PUBLISH_FAILED("error", "publish_failed"),
        TOPIC_NOT_CONFIGURED("error", "topic_not_configured"),
    }
}
