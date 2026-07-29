package no.fdk.harvester.metrics

import io.micrometer.core.instrument.Metrics
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import no.fdk.harvest.DataType
import no.fdk.harvester.error.HarvestErrorCategory
import no.fdk.harvester.model.FdkIdAndUri
import no.fdk.harvester.model.HarvestReport
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import kotlin.time.Duration.Companion.milliseconds

@Tag("unit")
class HarvestMetricsTest {
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
    fun `metricType maps legacy label values`() {
        assertEquals("dataset", HarvestMetrics.metricType(DataType.dataset))
        assertEquals("concept", HarvestMetrics.metricType(DataType.concept))
        assertEquals("dataservice", HarvestMetrics.metricType(DataType.dataservice))
        assertEquals("information-model", HarvestMetrics.metricType(DataType.informationmodel))
        assertEquals("public-service", HarvestMetrics.metricType(DataType.publicService))
        assertEquals("public-service", HarvestMetrics.metricType(DataType.service))
        assertEquals("event", HarvestMetrics.metricType(DataType.event))
    }

    @Test
    fun `record success increments harvest_count changed removed and timer`() {
        val report =
            HarvestReport(
                runId = "run-1",
                dataSourceId = "source-1",
                dataSourceUrl = "http://example.org/source",
                dataType = "dataset",
                harvestError = false,
                startTime = "2024-01-01T00:00:00+01:00",
                endTime = "2024-01-01T00:01:00+01:00",
                changedResources =
                    listOf(
                        FdkIdAndUri("a", "http://example.org/a"),
                        FdkIdAndUri("b", "http://example.org/b"),
                    ),
                removedResources = listOf(FdkIdAndUri("c", "http://example.org/c")),
            )

        HarvestMetrics.record(
            report = report,
            dataType = DataType.dataset,
            forceUpdate = true,
            dataSourceId = "source-1",
            dataSourceUrl = "http://example.org/source",
            duration = 150.milliseconds,
        )

        assertEquals(
            1.0,
            registry
                .counter(
                    "harvest_count",
                    "status",
                    "success",
                    "type",
                    "dataset",
                    "force_update",
                    "true",
                    "datasource_id",
                    "source-1",
                    "datasource_url",
                    "http://example.org/source",
                ).count(),
        )
        assertEquals(
            2.0,
            registry
                .counter(
                    "harvest_changed_resources_count",
                    "type",
                    "dataset",
                    "force_update",
                    "true",
                    "datasource_id",
                    "source-1",
                    "datasource_url",
                    "http://example.org/source",
                ).count(),
        )
        assertEquals(
            1.0,
            registry
                .counter(
                    "harvest_removed_resources_count",
                    "type",
                    "dataset",
                    "force_update",
                    "true",
                    "datasource_id",
                    "source-1",
                    "datasource_url",
                    "http://example.org/source",
                ).count(),
        )
        assertEquals(
            1L,
            registry
                .timer(
                    "harvest_time",
                    "type",
                    "dataset",
                    "force_update",
                    "true",
                    "datasource_id",
                    "source-1",
                    "datasource_url",
                    "http://example.org/source",
                ).count(),
        )
    }

    @Test
    fun `record harvestError only increments harvest_count with status error`() {
        val report =
            HarvestReport(
                runId = "run-1",
                dataSourceId = "source-1",
                dataSourceUrl = "http://example.org/source",
                dataType = "concept",
                harvestError = true,
                startTime = "2024-01-01T00:00:00+01:00",
                endTime = "2024-01-01T00:01:00+01:00",
                errorMessage = "boom",
                errorCategory = HarvestErrorCategory.SOURCE_UNAVAILABLE,
                changedResources = listOf(FdkIdAndUri("a", "http://example.org/a")),
            )

        HarvestMetrics.record(
            report = report,
            dataType = DataType.concept,
            forceUpdate = false,
            dataSourceId = "source-1",
            dataSourceUrl = "http://example.org/source",
            duration = 50.milliseconds,
        )

        assertEquals(
            1.0,
            registry
                .counter(
                    "harvest_count",
                    "status",
                    "error",
                    "type",
                    "concept",
                    "force_update",
                    "false",
                    "datasource_id",
                    "source-1",
                    "datasource_url",
                    "http://example.org/source",
                ).count(),
        )
        assertEquals(
            0.0,
            registry
                .find("harvest_changed_resources_count")
                .counter()
                ?.count() ?: 0.0,
        )
        assertEquals(0L, registry.find("harvest_time").timer()?.count() ?: 0L)
        assertEquals(
            1.0,
            registry
                .counter(
                    "harvest_error_count",
                    "category",
                    "source_unavailable",
                    "type",
                    "concept",
                ).count(),
        )
    }

    @Test
    fun `record null report increments harvest_count with status error`() {
        HarvestMetrics.record(
            report = null,
            dataType = DataType.dataset,
            forceUpdate = false,
            dataSourceId = "source-1",
            dataSourceUrl = "http://example.org/source",
            duration = 10.milliseconds,
        )

        assertEquals(
            1.0,
            registry
                .counter(
                    "harvest_count",
                    "status",
                    "error",
                    "type",
                    "dataset",
                    "force_update",
                    "false",
                    "datasource_id",
                    "source-1",
                    "datasource_url",
                    "http://example.org/source",
                ).count(),
        )
        assertEquals(0L, registry.find("harvest_time").timer()?.count() ?: 0L)
        assertEquals(
            1.0,
            registry
                .counter(
                    "harvest_error_count",
                    "category",
                    "internal_error",
                    "type",
                    "dataset",
                ).count(),
        )
    }

    @Test
    fun `recordError increments harvest_count and harvest_error_count`() {
        HarvestMetrics.recordError(
            dataType = DataType.informationmodel,
            forceUpdate = false,
            dataSourceId = "source-1",
            dataSourceUrl = "http://example.org/source",
            category = HarvestErrorCategory.VALIDATION_ERROR,
        )

        assertEquals(
            1.0,
            registry
                .counter(
                    "harvest_count",
                    "status",
                    "error",
                    "type",
                    "information-model",
                    "force_update",
                    "false",
                    "datasource_id",
                    "source-1",
                    "datasource_url",
                    "http://example.org/source",
                ).count(),
        )
        assertEquals(
            1.0,
            registry
                .counter(
                    "harvest_error_count",
                    "category",
                    "validation_error",
                    "type",
                    "information-model",
                ).count(),
        )
    }
}
