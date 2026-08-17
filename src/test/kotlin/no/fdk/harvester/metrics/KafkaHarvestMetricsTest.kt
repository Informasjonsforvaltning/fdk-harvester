package no.fdk.harvester.metrics

import io.micrometer.core.instrument.Metrics
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import no.fdk.harvest.DataType
import no.fdk.harvest.HarvestPhase
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test

@Tag("unit")
class KafkaHarvestMetricsTest {
    private lateinit var registry: SimpleMeterRegistry

    @BeforeEach
    fun setUp() {
        registry = SimpleMeterRegistry()
        Metrics.addRegistry(registry)
        KafkaHarvestMetrics.bind(Metrics.globalRegistry)
    }

    @AfterEach
    fun tearDown() {
        Metrics.removeRegistry(registry)
        registry.clear()
        KafkaHarvestMetrics.setListenerPaused(false)
    }

    @Test
    fun `recordEventProcessed increments harvest_event_processing_total`() {
        KafkaHarvestMetrics.recordEventProcessed(
            HarvestPhase.INITIATING,
            KafkaHarvestMetrics.EventProcessingResult.ACKED,
        )
        KafkaHarvestMetrics.recordEventProcessed(
            HarvestPhase.HARVESTING,
            KafkaHarvestMetrics.EventProcessingResult.SKIPPED,
        )
        KafkaHarvestMetrics.recordEventProcessed(
            HarvestPhase.REMOVING,
            KafkaHarvestMetrics.EventProcessingResult.NACKED,
        )

        assertEquals(
            1.0,
            registry
                .counter(
                    "harvest_event_processing_total",
                    "phase",
                    "initiating",
                    "result",
                    "acked",
                ).count(),
        )
        assertEquals(
            1.0,
            registry
                .counter(
                    "harvest_event_processing_total",
                    "phase",
                    "harvesting",
                    "result",
                    "skipped",
                ).count(),
        )
        assertEquals(
            1.0,
            registry
                .counter(
                    "harvest_event_processing_total",
                    "phase",
                    "removing",
                    "result",
                    "nacked",
                ).count(),
        )
    }

    @Test
    fun `bind exposes gauge before first state change`() {
        assertEquals(0.0, registry.find("kafka_listener_paused").gauge()?.value())
    }

    @Test
    fun `setListenerPaused updates kafka_listener_paused gauge`() {
        KafkaHarvestMetrics.setListenerPaused(true)
        assertEquals(1.0, registry.find("kafka_listener_paused").gauge()?.value())

        KafkaHarvestMetrics.setListenerPaused(false)
        assertEquals(0.0, registry.find("kafka_listener_paused").gauge()?.value())
    }

    @Test
    fun `bind registers again on a freshly attached registry`() {
        Metrics.removeRegistry(registry)
        registry.clear()
        val nextRegistry = SimpleMeterRegistry()
        Metrics.addRegistry(nextRegistry)
        try {
            KafkaHarvestMetrics.bind(Metrics.globalRegistry)
            KafkaHarvestMetrics.setListenerPaused(true)

            assertEquals(1.0, nextRegistry.find("kafka_listener_paused").gauge()?.value())
        } finally {
            Metrics.removeRegistry(nextRegistry)
            nextRegistry.clear()
            Metrics.addRegistry(registry)
        }
    }
}
