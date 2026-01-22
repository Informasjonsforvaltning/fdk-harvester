package no.fdk.fdk_harvester.kafka

import io.mockk.*
import no.fdk.fdk_harvester.model.FdkIdAndUri
import no.fdk.fdk_harvester.model.HarvestReport
import no.fdk.fdk_harvester.service.HarvestServiceApi
import no.fdk.harvest.DataType
import no.fdk.harvest.HarvestEvent
import no.fdk.harvest.HarvestPhase
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test

@Tag("unit")
class KafkaHarvestEventCircuitBreakerTest {

    private val harvestService: HarvestServiceApi = mockk(relaxed = true)
    private val harvestEventProducer: HarvestEventProducer = mockk(relaxed = true)
    private val resourceEventProducer: ResourceEventProducer = mockk(relaxed = true)

    private lateinit var circuitBreaker: KafkaHarvestEventCircuitBreaker

    @BeforeEach
    fun setUp() {
        clearAllMocks()
        circuitBreaker = KafkaHarvestEventCircuitBreaker(
            harvestService,
            harvestEventProducer,
            resourceEventProducer
        )
    }

    @Test
    fun `process delegates to harvest service for concept`() {
        val event = createHarvestEvent(DataType.concept, "source-1", "http://example.org/source")
        val record = ConsumerRecord("harvest-events", 0, 0L, "key", event)
        val report = createSuccessReport("concept", "source-1", "http://example.org/source")

        every { harvestService.executeHarvest(any(), any(), any(), any(), any(), any()) } returns report

        circuitBreaker.process(record)

        verify { harvestService.executeHarvest(
            dataSourceId = "source-1",
            dataSourceUrl = "http://example.org/source",
            dataType = DataType.concept,
            acceptHeader = "text/turtle",
            runId = "run-123",
            forced = false
        ) }
        verify { harvestEventProducer.produceHarvestingEvent(event, report) }
    }

    @Test
    fun `process delegates to harvest service for dataset`() {
        val event = createHarvestEvent(DataType.dataset, "source-1", "http://example.org/source")
        val record = ConsumerRecord("harvest-events", 0, 0L, "key", event)
        val report = createSuccessReport("dataset", "source-1", "http://example.org/source")

        every { harvestService.executeHarvest(any(), any(), any(), any(), any(), any()) } returns report

        circuitBreaker.process(record)

        verify { harvestService.executeHarvest(
            dataSourceId = "source-1",
            dataSourceUrl = "http://example.org/source",
            dataType = DataType.dataset,
            acceptHeader = "text/turtle",
            runId = "run-123",
            forced = false
        ) }
    }

    @Test
    fun `process uses forced flag from event`() {
        val event = createHarvestEvent(DataType.concept, "source-1", "http://example.org/source", forced = true)
        val record = ConsumerRecord("harvest-events", 0, 0L, "key", event)
        val report = createSuccessReport("concept", "source-1", "http://example.org/source")

        every { harvestService.executeHarvest(any(), any(), any(), any(), any(), any()) } returns report

        circuitBreaker.process(record)

        verify { harvestService.executeHarvest(
            dataSourceId = "source-1",
            dataSourceUrl = "http://example.org/source",
            dataType = DataType.concept,
            acceptHeader = "text/turtle",
            runId = "run-123",
            forced = true
        ) }
    }

    @Test
    fun `process ignores non-INITIATING phase and does not call service or producers`() {
        val event = HarvestEvent.newBuilder()
            .setPhase(HarvestPhase.HARVESTING)
            .setRunId("run-123")
            .setDataType(DataType.concept)
            .setDataSourceId("source-1")
            .setDataSourceUrl("http://example.org/source")
            .build()
        val record = ConsumerRecord("harvest-events", 0, 0L, "key", event)

        circuitBreaker.process(record)

        verify(exactly = 0) { harvestService.executeHarvest(any(), any(), any(), any(), any(), any()) }
        verify(exactly = 0) { harvestService.markResourcesAsDeleted(any(), any(), any(), any()) }
        verify(exactly = 0) { harvestEventProducer.produceHarvestingEvent(any(), any()) }
        verify(exactly = 0) { resourceEventProducer.publishHarvestedEvents(any(), any(), any(), any()) }
        verify(exactly = 0) { resourceEventProducer.publishRemovedEvents(any(), any(), any()) }
    }

    @Test
    fun `process skips when dataSourceId is null`() {
        val event = HarvestEvent.newBuilder()
            .setPhase(HarvestPhase.INITIATING)
            .setRunId("run-123")
            .setDataType(DataType.concept)
            .setDataSourceId(null)
            .setDataSourceUrl("http://example.org/source")
            .build()
        val record = ConsumerRecord("harvest-events", 0, 0L, "key", event)

        circuitBreaker.process(record)

        verify(exactly = 0) { harvestService.executeHarvest(any(), any(), any(), any(), any(), any()) }
    }

    @Test
    fun `process skips when dataSourceUrl is null`() {
        val event = HarvestEvent.newBuilder()
            .setPhase(HarvestPhase.INITIATING)
            .setRunId("run-123")
            .setDataType(DataType.concept)
            .setDataSourceId("source-1")
            .setDataSourceUrl(null)
            .build()
        val record = ConsumerRecord("harvest-events", 0, 0L, "key", event)

        circuitBreaker.process(record)

        verify(exactly = 0) { harvestService.executeHarvest(any(), any(), any(), any(), any(), any()) }
    }

    @Test
    fun `process handles exception from harvest service`() {
        val event = createHarvestEvent(DataType.concept, "source-1", "http://example.org/source")
        val record = ConsumerRecord("harvest-events", 0, 0L, "key", event)

        every { harvestService.executeHarvest(any(), any(), any(), any(), any(), any()) } throws RuntimeException("Test error")

        assertThrows(RuntimeException::class.java) {
            circuitBreaker.process(record)
        }

        verify { harvestService.executeHarvest(any(), any(), any(), any(), any(), any()) }
    }

    @Test
    fun `process handles error report`() {
        val event = createHarvestEvent(DataType.concept, "source-1", "http://example.org/source")
        val record = ConsumerRecord("harvest-events", 0, 0L, "key", event)
        val errorReport = HarvestReport(
            runId = "run-123",
            dataSourceId = "source-1",
            dataSourceUrl = "http://example.org/source",
            dataType = "concept",
            harvestError = true,
            startTime = "2024-01-01T00:00:00+01:00",
            endTime = "2024-01-01T00:01:00+01:00"
        )

        every { harvestService.executeHarvest(any(), any(), any(), any(), any(), any()) } returns errorReport

        circuitBreaker.process(record)

        verify { harvestService.executeHarvest(any(), any(), any(), any(), any(), any()) }
        verify { harvestEventProducer.produceHarvestingEvent(event, errorReport) }
    }

    @Test
    fun `process handles removeAll=true and marks resources as deleted`() {
        val event = createHarvestEvent(DataType.dataset, "source-1", "http://example.org/source", removeAll = true)
        val record = ConsumerRecord("harvest-events", 0, 0L, "key", event)
        val report = HarvestReport(
            runId = "run-123",
            dataSourceId = "source-1",
            dataSourceUrl = "http://example.org/source",
            dataType = "dataset",
            harvestError = false,
            startTime = "2024-01-01T00:00:00+01:00",
            endTime = "2024-01-01T00:01:00+01:00",
            removedResources = listOf(
                FdkIdAndUri("resource-1", "http://example.org/resource1"),
                FdkIdAndUri("resource-2", "http://example.org/resource2")
            )
        )

        every { harvestService.markResourcesAsDeleted(any(), any(), any(), any()) } returns report

        circuitBreaker.process(record)

        verify { harvestService.markResourcesAsDeleted(
            sourceUrl = "http://example.org/source",
            dataType = DataType.dataset,
            dataSourceId = "source-1",
            runId = "run-123"
        ) }
        verify { resourceEventProducer.publishRemovedEvents(
            dataType = DataType.dataset,
            resources = report.removedResources,
            runId = "run-123"
        ) }
        verify { harvestEventProducer.produceHarvestingEvent(event, report) }
    }

    @Test
    fun `process skips removeAll when dataSourceUrl is null`() {
        val event = HarvestEvent.newBuilder()
            .setPhase(HarvestPhase.INITIATING)
            .setRunId("run-123")
            .setDataType(DataType.concept)
            .setDataSourceId("source-1")
            .setDataSourceUrl(null)
            .setRemoveAll(true)
            .build()
        val record = ConsumerRecord("harvest-events", 0, 0L, "key", event)

        circuitBreaker.process(record)

        verify(exactly = 0) { harvestService.markResourcesAsDeleted(any(), any(), any(), any()) }
    }

    private fun createHarvestEvent(
        dataType: DataType,
        dataSourceId: String,
        dataSourceUrl: String,
        runId: String = "run-123",
        forced: Boolean = false,
        removeAll: Boolean = false
    ): HarvestEvent {
        return HarvestEvent.newBuilder()
            .setPhase(HarvestPhase.INITIATING)
            .setRunId(runId)
            .setDataType(dataType)
            .setDataSourceId(dataSourceId)
            .setDataSourceUrl(dataSourceUrl)
            .setAcceptHeader("text/turtle")
            .setForced(forced)
            .setRemoveAll(removeAll)
            .build()
    }

    private fun createSuccessReport(
        dataType: String,
        sourceId: String,
        sourceUrl: String
    ): HarvestReport {
        return HarvestReport(
            runId = "run-123",
            dataSourceId = sourceId,
            dataSourceUrl = sourceUrl,
            dataType = dataType,
            harvestError = false,
            startTime = "2024-01-01T00:00:00+01:00",
            endTime = "2024-01-01T00:01:00+01:00"
        )
    }
}
