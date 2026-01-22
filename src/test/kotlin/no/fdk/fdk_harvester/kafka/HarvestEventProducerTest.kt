package no.fdk.fdk_harvester.kafka

import io.mockk.*
import no.fdk.harvest.DataType
import no.fdk.harvest.HarvestEvent
import no.fdk.harvest.HarvestPhase
import org.apache.kafka.clients.producer.ProducerRecord
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.SendResult
import java.util.concurrent.CompletableFuture

@Tag("unit")
class HarvestEventProducerTest {

    private val kafkaTemplate: KafkaTemplate<String, HarvestEvent> = mockk()

    @Test
    fun `produceHarvestingEvent publishes HARVESTING phase event`() {
        val producer = HarvestEventProducer(kafkaTemplate, "harvest-events")

        val initiating = HarvestEvent.newBuilder()
            .setPhase(HarvestPhase.INITIATING)
            .setRunId("run-1")
            .setDataType(DataType.dataset)
            .setDataSourceId("ds-1")
            .setDataSourceUrl("http://example.org/source")
            .setAcceptHeader("text/turtle")
            .build()

        val future: CompletableFuture<SendResult<String, HarvestEvent>> = CompletableFuture.completedFuture(mockk())
        every { kafkaTemplate.send(any<String>(), any<String>(), any()) } returns future

        val result = producer.produceHarvestingEvent(initiating, report = null)

        assertNotNull(result)
        val keySlot = slot<String>()
        val eventSlot = slot<HarvestEvent>()
        verify(exactly = 1) { kafkaTemplate.send(eq("harvest-events"), capture(keySlot), capture(eventSlot)) }
        assertEquals("run-1", keySlot.captured.toString())
        assertEquals(HarvestPhase.HARVESTING, eventSlot.captured.phase)
        assertEquals(DataType.dataset, eventSlot.captured.dataType)
        assertEquals("ds-1", eventSlot.captured.dataSourceId?.toString())
    }

    @Test
    fun `produceHarvestingEvent returns null when runId is null`() {
        val producer = HarvestEventProducer(kafkaTemplate, "harvest-events")

        val initiating = HarvestEvent.newBuilder()
            .setPhase(HarvestPhase.INITIATING)
            .setRunId(null)
            .setDataType(DataType.dataset)
            .setDataSourceId("ds-1")
            .setDataSourceUrl("http://example.org/source")
            .build()

        val result = producer.produceHarvestingEvent(initiating, report = null)
        assertEquals(null, result)
        verify(exactly = 0) { kafkaTemplate.send(any<String>(), any<String>(), any()) }
    }

    @Test
    fun `produceHarvestingEvent with report passes report fields to event`() {
        val producer = HarvestEventProducer(kafkaTemplate, "harvest-events")
        val report = no.fdk.fdk_harvester.model.HarvestReport(
            runId = "run-1",
            dataSourceId = "ds-1",
            dataSourceUrl = "http://example.org/source",
            dataType = "dataset",
            harvestError = false,
            startTime = "2024-01-01T00:00:00",
            endTime = "2024-01-01T00:00:01",
            errorMessage = null,
            changedCatalogs = emptyList(),
            changedResources = listOf(no.fdk.fdk_harvester.model.FdkIdAndUri("fdk-1", "http://example.org/d1")),
            removedResources = listOf(no.fdk.fdk_harvester.model.FdkIdAndUri("fdk-2", "http://example.org/d2"))
        )

        val initiating = HarvestEvent.newBuilder()
            .setPhase(HarvestPhase.INITIATING)
            .setRunId("run-1")
            .setDataType(DataType.dataset)
            .setDataSourceId("ds-1")
            .setDataSourceUrl("http://example.org/source")
            .build()

        val future: CompletableFuture<SendResult<String, HarvestEvent>> = CompletableFuture.completedFuture(mockk())
        every { kafkaTemplate.send(any<String>(), any<String>(), any()) } returns future

        producer.produceHarvestingEvent(initiating, report = report)

        val eventSlot = slot<HarvestEvent>()
        verify(exactly = 1) { kafkaTemplate.send(eq("harvest-events"), eq("run-1"), capture(eventSlot)) }
        assertEquals(1, eventSlot.captured.changedResourcesCount)
        assertEquals(1, eventSlot.captured.removedResourcesCount)
        assertEquals("2024-01-01T00:00:00", eventSlot.captured.startTime?.toString())
        assertEquals("2024-01-01T00:00:01", eventSlot.captured.endTime?.toString())
    }

    @Test
    fun `produceHarvestingEvent returns null on exception`() {
        val producer = HarvestEventProducer(kafkaTemplate, "harvest-events")

        val initiating = HarvestEvent.newBuilder()
            .setPhase(HarvestPhase.INITIATING)
            .setDataType(DataType.dataset)
            .setDataSourceId("ds-1")
            .setDataSourceUrl("http://example.org/source")
            .build()

        every { kafkaTemplate.send(any<String>(), any<String>(), any()) } throws RuntimeException("boom")

        val result = producer.produceHarvestingEvent(initiating, report = null)
        assertEquals(null, result)
    }

    @Test
    fun `produceHarvestingEvent 9-arg overload sends event`() {
        val producer = HarvestEventProducer(kafkaTemplate, "harvest-events")
        val future: CompletableFuture<SendResult<String, HarvestEvent>> = CompletableFuture.completedFuture(mockk())
        every { kafkaTemplate.send(any<String>(), any<String>(), any()) } returns future

        producer.produceHarvestingEvent(
            runId = "run-1",
            dataType = DataType.dataset,
            dataSourceId = "ds-1",
            dataSourceUrl = "http://example.org/source",
            startTime = "2024-01-01T00:00:00",
            endTime = "2024-01-01T00:00:01",
            errorMessage = null,
            changedResourcesCount = 5,
            removedResourcesCount = 2
        )

        val eventSlot = slot<HarvestEvent>()
        verify(exactly = 1) { kafkaTemplate.send(eq("harvest-events"), eq("run-1"), capture(eventSlot)) }
        assertEquals(HarvestPhase.HARVESTING, eventSlot.captured.phase)
        assertEquals("run-1", eventSlot.captured.runId?.toString())
        assertEquals(DataType.dataset, eventSlot.captured.dataType)
        assertEquals(5, eventSlot.captured.changedResourcesCount)
        assertEquals(2, eventSlot.captured.removedResourcesCount)
    }

    @Test
    fun `produceHarvestingEvent 9-arg overload handles exception`() {
        val producer = HarvestEventProducer(kafkaTemplate, "harvest-events")
        every { kafkaTemplate.send(any<String>(), any<String>(), any()) } throws RuntimeException("send failed")

        producer.produceHarvestingEvent(
            runId = "run-1",
            dataType = DataType.concept,
            dataSourceId = "ds-1",
            dataSourceUrl = "http://example.org/source",
            startTime = "2024-01-01T00:00:00",
            endTime = "2024-01-01T00:00:01",
            errorMessage = "harvest failed",
            changedResourcesCount = 0,
            removedResourcesCount = 0
        )
        verify(exactly = 1) { kafkaTemplate.send(any(), any(), any()) }
    }
}


