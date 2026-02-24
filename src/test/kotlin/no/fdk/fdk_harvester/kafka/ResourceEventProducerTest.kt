package no.fdk.fdk_harvester.kafka

import io.mockk.*
import no.fdk.concept.ConceptEvent
import no.fdk.concept.ConceptEventType
import no.fdk.dataservice.DataServiceEvent
import no.fdk.dataservice.DataServiceEventType
import no.fdk.dataset.DatasetEvent
import no.fdk.dataset.DatasetEventType
import no.fdk.event.EventEvent
import no.fdk.event.EventEventType
import no.fdk.fdk_harvester.model.FdkIdAndUri
import no.fdk.harvest.DataType
import no.fdk.informationmodel.InformationModelEvent
import no.fdk.informationmodel.InformationModelEventType
import no.fdk.service.ServiceEvent
import no.fdk.service.ServiceEventType
import org.apache.avro.io.DecoderFactory
import org.apache.avro.io.EncoderFactory
import org.apache.avro.specific.SpecificDatumReader
import org.apache.avro.specific.SpecificDatumWriter
import org.apache.avro.specific.SpecificRecord
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.SendResult
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.util.concurrent.CompletableFuture

@Tag("unit")
class ResourceEventProducerTest {

    private val kafkaTemplate: KafkaTemplate<String, SpecificRecord> = mockk()

    @Test
    fun `publishHarvestedEvents sends harvested record to dataset topic`() {
        val producer = ResourceEventProducer(
            kafkaTemplate = kafkaTemplate,
            datasetEventsTopic = "dataset-events",
            conceptEventsTopic = "concept-events",
            dataServiceEventsTopic = "data-service-events",
            informationModelEventsTopic = "information-model-events",
            serviceEventsTopic = "service-events",
            eventEventsTopic = "event-events"
        )

        val resources = listOf(FdkIdAndUri(fdkId = "fdk-1", uri = "http://example.org/ds1"))
        val graphs = mapOf("fdk-1" to "<ttl>")

        every { kafkaTemplate.send(any<String>(), any<String>(), any()) } returns CompletableFuture.completedFuture(mockk<SendResult<String, SpecificRecord>>())
        producer.publishHarvestedEvents(DataType.dataset, resources, graphs, runId = "run-1")

        val recordSlot = slot<SpecificRecord>()
        verify(exactly = 1) { kafkaTemplate.send(eq("dataset-events"), eq("fdk-1"), capture(recordSlot)) }

        val record = recordSlot.captured as DatasetEvent
        assertEquals(DatasetEventType.DATASET_HARVESTED, record.type)
        assertEquals("run-1", record.harvestRunId?.toString())
        assertEquals("http://example.org/ds1", record.uri?.toString())
        assertEquals("fdk-1", record.fdkId.toString())
        assertEquals("<ttl>", record.graph.toString())
    }

    @Test
    fun `DatasetEvent round-trip serialization preserves type fdkId and graph`() {
        val graphWithDataset = "@prefix dcat: <http://www.w3.org/ns/dcat#> . _:d a dcat:Dataset ."
        val event = DatasetEvent.newBuilder()
            .setType(DatasetEventType.DATASET_HARVESTED)
            .setHarvestRunId("run-1")
            .setUri("http://example.org/ds1")
            .setFdkId("fdk-1")
            .setGraph(graphWithDataset)
            .setTimestamp(12345L)
            .build()

        val bytes = ByteArrayOutputStream().use { out ->
            val writer = SpecificDatumWriter<DatasetEvent>(DatasetEvent::class.java)
            val encoder = EncoderFactory.get().binaryEncoder(out, null)
            writer.write(event, encoder)
            encoder.flush()
            out.toByteArray()
        }

        val decoded = ByteArrayInputStream(bytes).use { input ->
            val reader = SpecificDatumReader<DatasetEvent>(DatasetEvent::class.java)
            val decoder = DecoderFactory.get().binaryDecoder(input, null)
            reader.read(null, decoder)
        }

        assertNotNull(decoded.type, "type should not be null after round-trip")
        assertNotNull(decoded.fdkId, "fdkId should not be null after round-trip")
        assertNotNull(decoded.graph, "graph should not be null after round-trip")
        assertEquals(DatasetEventType.DATASET_HARVESTED, decoded.type)
        assertEquals("fdk-1", decoded.fdkId.toString())
        assertEquals(graphWithDataset, decoded.graph.toString())
        assertEquals("run-1", decoded.harvestRunId?.toString())
        assertEquals("http://example.org/ds1", decoded.uri?.toString())
    }

    @Test
    fun `publishRemovedEvents sends removed record to dataset topic`() {
        val producer = ResourceEventProducer(
            kafkaTemplate = kafkaTemplate,
            datasetEventsTopic = "dataset-events",
            conceptEventsTopic = "concept-events",
            dataServiceEventsTopic = "data-service-events",
            informationModelEventsTopic = "information-model-events",
            serviceEventsTopic = "service-events",
            eventEventsTopic = "event-events"
        )

        val resources = listOf(FdkIdAndUri(fdkId = "fdk-1", uri = "http://example.org/ds1"))

        every { kafkaTemplate.send(any<String>(), any<String>(), any()) } returns CompletableFuture.completedFuture(mockk<SendResult<String, SpecificRecord>>())
        producer.publishRemovedEvents(DataType.dataset, resources, runId = "run-1")

        val recordSlot = slot<SpecificRecord>()
        verify(exactly = 1) { kafkaTemplate.send(eq("dataset-events"), eq("fdk-1"), capture(recordSlot)) }

        val record = recordSlot.captured as DatasetEvent
        assertEquals(DatasetEventType.DATASET_REMOVED, record.type)
        assertEquals("run-1", record.harvestRunId?.toString())
        assertEquals("http://example.org/ds1", record.uri?.toString())
        assertEquals("fdk-1", record.fdkId.toString())
        assertEquals("", record.graph.toString())
    }

    @Test
    fun `publishHarvestedEvents covers all DataType branches`() {
        val producer = ResourceEventProducer(
            kafkaTemplate = kafkaTemplate,
            datasetEventsTopic = "dataset-events",
            conceptEventsTopic = "concept-events",
            dataServiceEventsTopic = "data-service-events",
            informationModelEventsTopic = "information-model-events",
            serviceEventsTopic = "service-events",
            eventEventsTopic = "event-events"
        )

        val topics = mutableListOf<String>()
        val keys = mutableListOf<String>()
        val records = mutableListOf<SpecificRecord>()
        every { kafkaTemplate.send(capture(topics), capture(keys), capture(records)) } returns CompletableFuture.completedFuture(mockk<SendResult<String, SpecificRecord>>())

        fun run(dataType: DataType, expectedTopic: String) {
            topics.clear(); keys.clear(); records.clear()
            producer.publishHarvestedEvents(
                dataType = dataType,
                resources = listOf(FdkIdAndUri(fdkId = "fdk-1", uri = "http://example.org/u")),
                resourceGraphs = mapOf("fdk-1" to "<g>"),
                runId = "run-1"
            )
            assertEquals(expectedTopic, topics.single())
            assertEquals("fdk-1", keys.single())
            assertEquals(1, records.size)
        }

        run(DataType.dataset, "dataset-events")
        assertEquals(DatasetEventType.DATASET_HARVESTED, (records.single() as DatasetEvent).type)

        run(DataType.concept, "concept-events")
        assertEquals(ConceptEventType.CONCEPT_HARVESTED, (records.single() as ConceptEvent).type)

        run(DataType.dataservice, "data-service-events")
        assertEquals(DataServiceEventType.DATA_SERVICE_HARVESTED, (records.single() as DataServiceEvent).type)

        run(DataType.informationmodel, "information-model-events")
        assertEquals(InformationModelEventType.INFORMATION_MODEL_HARVESTED, (records.single() as InformationModelEvent).type)

        run(DataType.service, "service-events")
        assertEquals(ServiceEventType.SERVICE_HARVESTED, (records.single() as ServiceEvent).type)

        run(DataType.publicService, "service-events")
        assertEquals(ServiceEventType.SERVICE_HARVESTED, (records.single() as ServiceEvent).type)

        run(DataType.event, "event-events")
        assertEquals(EventEventType.EVENT_HARVESTED, (records.single() as EventEvent).type)
    }

    @Test
    fun `publishRemovedEvents covers all DataType branches`() {
        val producer = ResourceEventProducer(
            kafkaTemplate = kafkaTemplate,
            datasetEventsTopic = "dataset-events",
            conceptEventsTopic = "concept-events",
            dataServiceEventsTopic = "dataservice-events",
            informationModelEventsTopic = "information-model-events",
            serviceEventsTopic = "service-events",
            eventEventsTopic = "event-events"
        )

        val topics = mutableListOf<String>()
        val keys = mutableListOf<String>()
        val records = mutableListOf<SpecificRecord>()
        every { kafkaTemplate.send(capture(topics), capture(keys), capture(records)) } returns CompletableFuture.completedFuture(mockk<SendResult<String, SpecificRecord>>())

        fun run(dataType: DataType, expectedTopic: String) {
            topics.clear(); keys.clear(); records.clear()
            producer.publishRemovedEvents(
                dataType = dataType,
                resources = listOf(FdkIdAndUri(fdkId = "fdk-1", uri = "http://example.org/u")),
                runId = "run-1"
            )
            assertEquals(expectedTopic, topics.single())
            assertEquals("fdk-1", keys.single())
            assertEquals(1, records.size)
        }

        run(DataType.dataset, "dataset-events")
        assertEquals(DatasetEventType.DATASET_REMOVED, (records.single() as DatasetEvent).type)

        run(DataType.concept, "concept-events")
        assertEquals(ConceptEventType.CONCEPT_REMOVED, (records.single() as ConceptEvent).type)

        run(DataType.dataservice, "dataservice-events")
        assertEquals(DataServiceEventType.DATA_SERVICE_REMOVED, (records.single() as DataServiceEvent).type)

        run(DataType.informationmodel, "information-model-events")
        assertEquals(InformationModelEventType.INFORMATION_MODEL_REMOVED, (records.single() as InformationModelEvent).type)

        run(DataType.service, "service-events")
        assertEquals(ServiceEventType.SERVICE_REMOVED, (records.single() as ServiceEvent).type)

        run(DataType.publicService, "service-events")
        assertEquals(ServiceEventType.SERVICE_REMOVED, (records.single() as ServiceEvent).type)

        run(DataType.event, "event-events")
        assertEquals(EventEventType.EVENT_REMOVED, (records.single() as EventEvent).type)
    }
}


