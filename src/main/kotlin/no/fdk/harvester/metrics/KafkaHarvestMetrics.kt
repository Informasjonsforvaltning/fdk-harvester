package no.fdk.harvester.metrics

import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Metrics
import no.fdk.harvest.HarvestPhase
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

object KafkaHarvestMetrics {
    private var registry: MeterRegistry = Metrics.globalRegistry
    private val listenerPaused = AtomicInteger(0)
    private val gaugeRegistered = AtomicBoolean(false)

    fun bind(registry: MeterRegistry) {
        this.registry = registry
    }

    fun registerListenerPausedGauge() {
        ensureListenerPausedGaugeRegistered()
    }

    fun recordEventProcessed(phase: HarvestPhase, result: EventProcessingResult) {
        registry
            .counter(
                "harvest_event_processing_total",
                "phase",
                phase.name.lowercase(),
                "result",
                result.label,
            ).increment()
    }

    fun setListenerPaused(paused: Boolean) {
        ensureListenerPausedGaugeRegistered()
        listenerPaused.set(if (paused) 1 else 0)
    }

    private fun ensureListenerPausedGaugeRegistered() {
        if (gaugeRegistered.compareAndSet(false, true)) {
            Gauge
                .builder("kafka_listener_paused") { listenerPaused.get().toDouble() }
                .description("1 when the harvest Kafka listener is paused, otherwise 0")
                .tag("listener", "harvest")
                .register(registry)
        }
    }

    enum class EventProcessingResult(val label: String) {
        ACKED("acked"),
        NACKED("nacked"),
        SKIPPED("skipped"),
        CIRCUIT_OPEN("circuit_open"),
    }
}
