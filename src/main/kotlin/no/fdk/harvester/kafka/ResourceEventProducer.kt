package no.fdk.harvester.kafka

import no.fdk.concept.ConceptEvent
import no.fdk.concept.ConceptEventType
import no.fdk.dataservice.DataServiceEvent
import no.fdk.dataservice.DataServiceEventType
import no.fdk.dataset.DatasetEvent
import no.fdk.dataset.DatasetEventType
import no.fdk.event.EventEvent
import no.fdk.event.EventEventType
import no.fdk.harvest.DataType
import no.fdk.harvester.model.FdkIdAndUri
import no.fdk.informationmodel.InformationModelEvent
import no.fdk.informationmodel.InformationModelEventType
import no.fdk.service.ServiceEvent
import no.fdk.service.ServiceEventType
import org.apache.avro.specific.SpecificRecord
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Service

/**
 * Publishes type-specific Avro events to Kafka for harvested and removed resources.
 *
 * [DataType] maps to a topic via [topicByDataType]; [DataType.publicService] shares the service topic.
 */
@Service
class ResourceEventProducer(
    private val kafkaTemplate: KafkaTemplate<String, SpecificRecord>,
    @Value("\${app.kafka.topic.dataset-events:dataset-events}") datasetEventsTopic: String,
    @Value("\${app.kafka.topic.concept-events:concept-events}") conceptEventsTopic: String,
    @Value("\${app.kafka.topic.dataservice-events:dataservice-events}") dataServiceEventsTopic: String,
    @Value("\${app.kafka.topic.informationmodel-events:informationmodel-events}") informationModelEventsTopic: String,
    @Value("\${app.kafka.topic.service-events:service-events}") serviceEventsTopic: String,
    @Value("\${app.kafka.topic.event-events:event-events}") eventEventsTopic: String,
) {
    private val logger = LoggerFactory.getLogger(ResourceEventProducer::class.java)

    private val topicByDataType: Map<DataType, String> =
        mapOf(
            DataType.dataset to datasetEventsTopic,
            DataType.concept to conceptEventsTopic,
            DataType.dataservice to dataServiceEventsTopic,
            DataType.informationmodel to informationModelEventsTopic,
            DataType.service to serviceEventsTopic,
            DataType.publicService to serviceEventsTopic,
            DataType.event to eventEventsTopic,
        )

    fun publishHarvestedEvents(
        dataType: DataType,
        resources: List<FdkIdAndUri>,
        resourceGraphs: Map<String, String>,
        runId: String,
        catalogGraphs: Map<String, String>,
    ) {
        publishEvents(dataType, resources, runId, ResourceEventKind.HARVESTED, resourceGraphs, catalogGraphs)
    }

    fun publishRemovedEvents(
        dataType: DataType,
        resources: List<FdkIdAndUri>,
        runId: String,
    ) {
        publishEvents(dataType, resources, runId, ResourceEventKind.REMOVED)
    }

    private fun publishEvents(
        dataType: DataType,
        resources: List<FdkIdAndUri>,
        runId: String,
        kind: ResourceEventKind,
        resourceGraphs: Map<String, String> = emptyMap(),
        catalogGraphs: Map<String, String> = emptyMap(),
    ) {
        val topicName =
            topicByDataType[dataType] ?: run {
                logger.warn("No topic configured for dataType: $dataType")
                return
            }

        resources.forEach { resource ->
            try {
                val graph =
                    when (kind) {
                        ResourceEventKind.HARVESTED -> resourceGraphs[resource.fdkId] ?: ""
                        ResourceEventKind.REMOVED -> ""
                    }
                val catalogGraph =
                    when (kind) {
                        ResourceEventKind.HARVESTED -> catalogGraphs[resource.fdkId] ?: ""
                        ResourceEventKind.REMOVED -> ""
                    }
                val event = buildEvent(dataType, resource, runId, graph, kind, catalogGraph)
                kafkaTemplate.send(topicName, resource.fdkId, event)
                logProducedResourceEvent(
                    topic = topicName,
                    key = resource.fdkId,
                    eventType = "${dataType.name.uppercase()}_${kind.name}",
                    fdkId = resource.fdkId,
                    uri = resource.uri,
                    runId = runId,
                    graph = graph,
                )
            } catch (e: Exception) {
                logger.error(
                    "Failed to produce ${dataType.name.uppercase()}_${kind.name} event for fdkId: ${resource.fdkId}, runId: $runId",
                    e,
                )
            }
        }
    }

    private fun buildEvent(
        dataType: DataType,
        resource: FdkIdAndUri,
        runId: String,
        graph: String,
        kind: ResourceEventKind,
        catalogGraph: String,
    ): SpecificRecord {
        val timestamp = System.currentTimeMillis()

        return when (dataType) {
            DataType.dataset -> {
                val eventType =
                    when (kind) {
                        ResourceEventKind.HARVESTED -> DatasetEventType.DATASET_HARVESTED
                        ResourceEventKind.REMOVED -> DatasetEventType.DATASET_REMOVED
                    }
                DatasetEvent
                    .newBuilder()
                    .setType(eventType)
                    .setHarvestRunId(runId)
                    .setUri(resource.uri)
                    .setFdkId(resource.fdkId)
                    .setGraph(graph)
                    .setTimestamp(timestamp)
                    .setCatalogGraph(catalogGraph)
                    .build()
            }

            DataType.concept -> {
                val eventType =
                    when (kind) {
                        ResourceEventKind.HARVESTED -> ConceptEventType.CONCEPT_HARVESTED
                        ResourceEventKind.REMOVED -> ConceptEventType.CONCEPT_REMOVED
                    }
                ConceptEvent
                    .newBuilder()
                    .setType(eventType)
                    .setHarvestRunId(runId)
                    .setUri(resource.uri)
                    .setFdkId(resource.fdkId)
                    .setGraph(graph)
                    .setTimestamp(timestamp)
                    .setCatalogGraph(catalogGraph)
                    .build()
            }

            DataType.dataservice -> {
                val eventType =
                    when (kind) {
                        ResourceEventKind.HARVESTED -> DataServiceEventType.DATA_SERVICE_HARVESTED
                        ResourceEventKind.REMOVED -> DataServiceEventType.DATA_SERVICE_REMOVED
                    }
                DataServiceEvent
                    .newBuilder()
                    .setType(eventType)
                    .setHarvestRunId(runId)
                    .setUri(resource.uri)
                    .setFdkId(resource.fdkId)
                    .setGraph(graph)
                    .setTimestamp(timestamp)
                    .setCatalogGraph(catalogGraph)
                    .build()
            }

            DataType.informationmodel -> {
                val eventType =
                    when (kind) {
                        ResourceEventKind.HARVESTED -> InformationModelEventType.INFORMATION_MODEL_HARVESTED
                        ResourceEventKind.REMOVED -> InformationModelEventType.INFORMATION_MODEL_REMOVED
                    }
                InformationModelEvent
                    .newBuilder()
                    .setType(eventType)
                    .setHarvestRunId(runId)
                    .setUri(resource.uri)
                    .setFdkId(resource.fdkId)
                    .setGraph(graph)
                    .setTimestamp(timestamp)
                    .setCatalogGraph(catalogGraph)
                    .build()
            }

            DataType.service, DataType.publicService -> {
                val eventType =
                    when (kind) {
                        ResourceEventKind.HARVESTED -> ServiceEventType.SERVICE_HARVESTED
                        ResourceEventKind.REMOVED -> ServiceEventType.SERVICE_REMOVED
                    }
                ServiceEvent
                    .newBuilder()
                    .setType(eventType)
                    .setHarvestRunId(runId)
                    .setUri(resource.uri)
                    .setFdkId(resource.fdkId)
                    .setGraph(graph)
                    .setTimestamp(timestamp)
                    .setCatalogGraph(catalogGraph)
                    .build()
            }

            DataType.event -> {
                val eventType =
                    when (kind) {
                        ResourceEventKind.HARVESTED -> EventEventType.EVENT_HARVESTED
                        ResourceEventKind.REMOVED -> EventEventType.EVENT_REMOVED
                    }
                EventEvent
                    .newBuilder()
                    .setType(eventType)
                    .setHarvestRunId(runId)
                    .setUri(resource.uri)
                    .setFdkId(resource.fdkId)
                    .setGraph(graph)
                    .setTimestamp(timestamp)
                    .setCatalogGraph(catalogGraph)
                    .build()
            }
        }
    }

    private fun logProducedResourceEvent(
        topic: String,
        key: String,
        eventType: String,
        fdkId: String,
        uri: String,
        runId: String,
        graph: String,
    ) {
        if (!logger.isDebugEnabled) return
        logger.debug(
            "Produced resource event: topic=$topic, key=$key, eventType=$eventType, fdkId=$fdkId, uri=$uri, runId=$runId, graph=$graph",
        )
    }

    private enum class ResourceEventKind {
        HARVESTED,
        REMOVED,
    }
}
