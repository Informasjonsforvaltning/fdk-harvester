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
 * Publishes type-specific Avro events (dataset, concept, dataservice, informationmodel, service, event)
 * to the corresponding Kafka topics for harvested and removed resources.
 */
@Service
class ResourceEventProducer(
    private val kafkaTemplate: KafkaTemplate<String, SpecificRecord>,
    @param:Value("\${app.kafka.topic.dataset-events:dataset-events}") datasetEventsTopic: String,
    @param:Value("\${app.kafka.topic.concept-events:concept-events}") conceptEventsTopic: String,
    @param:Value("\${app.kafka.topic.dataservice-events:dataservice-events}") dataServiceEventsTopic: String,
    @param:Value("\${app.kafka.topic.informationmodel-events:informationmodel-events}") informationModelEventsTopic: String,
    @param:Value("\${app.kafka.topic.service-events:service-events}") serviceEventsTopic: String,
    @param:Value("\${app.kafka.topic.event-events:event-events}") eventEventsTopic: String,
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
    ) {
        publishEvents(dataType, resources, runId, ResourceEventKind.HARVESTED, resourceGraphs)
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
                val event = buildEvent(dataType, resource, runId, graph, kind)
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
                    .build()
            }

            DataType.concept -> {
                val eventType =
                    when (kind) {
                        ResourceEventKind.HARVESTED -> ConceptEventType.CONCEPT_HARVESTED
                        ResourceEventKind.REMOVED -> ConceptEventType.CONCEPT_REMOVED
                    }
                ConceptEvent(eventType, runId, resource.uri, resource.fdkId, graph, timestamp)
            }

            DataType.dataservice -> {
                val eventType =
                    when (kind) {
                        ResourceEventKind.HARVESTED -> DataServiceEventType.DATA_SERVICE_HARVESTED
                        ResourceEventKind.REMOVED -> DataServiceEventType.DATA_SERVICE_REMOVED
                    }
                DataServiceEvent(eventType, runId, resource.uri, resource.fdkId, graph, timestamp)
            }

            DataType.informationmodel -> {
                val eventType =
                    when (kind) {
                        ResourceEventKind.HARVESTED -> InformationModelEventType.INFORMATION_MODEL_HARVESTED
                        ResourceEventKind.REMOVED -> InformationModelEventType.INFORMATION_MODEL_REMOVED
                    }
                InformationModelEvent(eventType, runId, resource.uri, resource.fdkId, graph, timestamp)
            }

            DataType.service, DataType.publicService -> {
                val eventType =
                    when (kind) {
                        ResourceEventKind.HARVESTED -> ServiceEventType.SERVICE_HARVESTED
                        ResourceEventKind.REMOVED -> ServiceEventType.SERVICE_REMOVED
                    }
                ServiceEvent(eventType, runId, resource.uri, resource.fdkId, graph, timestamp)
            }

            DataType.event -> {
                val eventType =
                    when (kind) {
                        ResourceEventKind.HARVESTED -> EventEventType.EVENT_HARVESTED
                        ResourceEventKind.REMOVED -> EventEventType.EVENT_REMOVED
                    }
                EventEvent(eventType, runId, resource.uri, resource.fdkId, graph, timestamp)
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
