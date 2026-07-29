package no.fdk.harvester.metrics

import io.micrometer.core.instrument.Metrics
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import no.fdk.harvest.DataType
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test

@Tag("unit")
class ResourceEventMetricsTest {
    private lateinit var registry: SimpleMeterRegistry

    @BeforeEach
    fun setUp() {
        registry = SimpleMeterRegistry()
        Metrics.addRegistry(registry)
    }

    @AfterEach
    fun tearDown() {
        Metrics.removeRegistry(registry)
        registry.clear()
    }

    @Test
    fun `recordPublish increments resource_event_publish_total`() {
        ResourceEventMetrics.recordPublish(
            dataType = DataType.dataset,
            kind = ResourceEventMetrics.ResourceEventKind.HARVESTED,
            success = true,
        )
        ResourceEventMetrics.recordPublish(
            dataType = DataType.publicService,
            kind = ResourceEventMetrics.ResourceEventKind.REMOVED,
            success = false,
        )

        assertEquals(
            1.0,
            registry
                .counter(
                    "resource_event_publish_total",
                    "status",
                    "success",
                    "type",
                    "dataset",
                    "kind",
                    "harvested",
                ).count(),
        )
        assertEquals(
            1.0,
            registry
                .counter(
                    "resource_event_publish_total",
                    "status",
                    "error",
                    "type",
                    "public-service",
                    "kind",
                    "removed",
                ).count(),
        )
    }
}
