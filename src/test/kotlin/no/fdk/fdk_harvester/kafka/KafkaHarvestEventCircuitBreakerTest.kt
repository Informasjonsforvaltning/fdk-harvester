package no.fdk.fdk_harvester.kafka

import com.fasterxml.jackson.module.kotlin.kotlinModule
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
    fun `process REMOVING phase delegates to markResourceAsDeletedByFdkId and publishes events`() {
        val event = createRemovingEvent(DataType.dataset, "source-1", "http://example.org/source", fdkId = "fdk-123")
        val record = ConsumerRecord("harvest-events", 0, 0L, "key", event)
        val report = HarvestReport(
            runId = "run-123",
            dataSourceId = "source-1",
            dataSourceUrl = "http://example.org/source",
            dataType = "dataset",
            harvestError = false,
            startTime = "2024-01-01T00:00:00+01:00",
            endTime = "2024-01-01T00:01:00+01:00",
            removedResources = listOf(FdkIdAndUri("fdk-123", "http://example.org/resource1"))
        )

        every { harvestService.markResourceAsDeletedByFdkId(any(), any(), any(), any(), any()) } returns report

        circuitBreaker.process(record)

        verify { harvestService.markResourceAsDeletedByFdkId(
            fdkId = "fdk-123",
            uri = "fdk-123",
            dataType = DataType.dataset,
            runId = "run-123",
            dataSourceId = "source-1"
        ) }
        verify { resourceEventProducer.publishRemovedEvents(
            dataType = DataType.dataset,
            resources = report.removedResources,
            runId = "run-123"
        ) }
        verify { harvestEventProducer.produceHarvestingEvent(event, report) }
    }

    @Test
    fun `process REMOVING phase with missing fdkId catches IllegalArgumentException and produces error event`() {
        val event = HarvestEvent.newBuilder()
            .setPhase(HarvestPhase.REMOVING)
            .setRunId("run-123")
            .setDataType(DataType.concept)
            .setDataSourceId("source-1")
            .setDataSourceUrl("http://example.org/source")
            .setFdkId(null)
            .build()
        val record = ConsumerRecord("harvest-events", 0, 0L, "key", event)

        circuitBreaker.process(record)

        verify(exactly = 0) { harvestService.markResourceAsDeletedByFdkId(any(), any(), any(), any(), any()) }
    }

    @Test
    fun `process REMOVING phase with missing runId catches IllegalArgumentException and produces error event`() {
        val event = HarvestEvent.newBuilder()
            .setPhase(HarvestPhase.REMOVING)
            .setRunId(null)
            .setDataType(DataType.concept)
            .setDataSourceId("source-1")
            .setDataSourceUrl("http://example.org/source")
            .setFdkId("fdk-123")
            .build()
        val record = ConsumerRecord("harvest-events", 0, 0L, "key", event)

        circuitBreaker.process(record)

        verify(exactly = 0) { harvestService.markResourceAsDeletedByFdkId(any(), any(), any(), any(), any()) }
    }

    @Test
    fun `process REMOVING phase with missing dataSourceId catches IllegalArgumentException`() {
        val event = HarvestEvent.newBuilder()
            .setPhase(HarvestPhase.REMOVING)
            .setRunId("run-123")
            .setDataType(DataType.concept)
            .setDataSourceId(null)
            .setDataSourceUrl("http://example.org/source")
            .setFdkId("fdk-123")
            .build()
        val record = ConsumerRecord("harvest-events", 0, 0L, "key", event)

        circuitBreaker.process(record)

        verify(exactly = 0) { harvestService.markResourceAsDeletedByFdkId(any(), any(), any(), any(), any()) }
    }

    @Test
    fun `process REMOVING phase handles exception from harvest service`() {
        val event = createRemovingEvent(DataType.dataset, "source-1", "http://example.org/source", fdkId = "fdk-123")
        val record = ConsumerRecord("harvest-events", 0, 0L, "key", event)

        every { harvestService.markResourceAsDeletedByFdkId(any(), any(), any(), any(), any()) } throws RuntimeException("DB error")

        assertThrows(RuntimeException::class.java) {
            circuitBreaker.process(record)
        }

        verify { harvestService.markResourceAsDeletedByFdkId(any(), any(), any(), any(), any()) }
    }

    @Test
    fun `process REMOVING phase for different data types`() {
        for (dataType in listOf(DataType.concept, DataType.dataset, DataType.dataservice, DataType.informationmodel, DataType.service, DataType.event)) {
            clearAllMocks()

            val event = createRemovingEvent(dataType, "source-1", "http://example.org/source", fdkId = "fdk-456")
            val record = ConsumerRecord("harvest-events", 0, 0L, "key", event)
            val report = HarvestReport(
                runId = "run-123",
                dataSourceId = "source-1",
                dataSourceUrl = "http://example.org/source",
                dataType = dataType.name.lowercase(),
                harvestError = false,
                startTime = "2024-01-01T00:00:00+01:00",
                endTime = "2024-01-01T00:01:00+01:00",
                removedResources = listOf(FdkIdAndUri("fdk-456", "http://example.org/resource"))
            )

            every { harvestService.markResourceAsDeletedByFdkId(any(), any(), any(), any(), any()) } returns report

            circuitBreaker.process(record)

            verify { harvestService.markResourceAsDeletedByFdkId(
                fdkId = "fdk-456",
                uri = "fdk-456",
                dataType = dataType,
                runId = "run-123",
                dataSourceId = "source-1"
            ) }
        }
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

    private fun createRemovingEvent(
        dataType: DataType,
        dataSourceId: String,
        dataSourceUrl: String,
        fdkId: String,
        runId: String = "run-123"
    ): HarvestEvent {
        return HarvestEvent.newBuilder()
            .setPhase(HarvestPhase.REMOVING)
            .setRunId(runId)
            .setDataType(dataType)
            .setDataSourceId(dataSourceId)
            .setDataSourceUrl(dataSourceUrl)
            .setFdkId(fdkId)
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
