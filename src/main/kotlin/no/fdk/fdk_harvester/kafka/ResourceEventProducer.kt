package no.fdk.fdk_harvester.kafka

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
    @param:Value("\${app.kafka.topic.dataset-events:dataset-events}") private val datasetEventsTopic: String,
    @param:Value("\${app.kafka.topic.concept-events:concept-events}") private val conceptEventsTopic: String,
    @param:Value("\${app.kafka.topic.dataservice-events:dataservice-events}") private val dataServiceEventsTopic: String,
    @param:Value("\${app.kafka.topic.informationmodel-events:informationmodel-events}") private val informationModelEventsTopic: String,
    @param:Value("\${app.kafka.topic.service-events:service-events}") private val serviceEventsTopic: String,
    @param:Value("\${app.kafka.topic.event-events:event-events}") private val eventEventsTopic: String
) {
    private val logger = LoggerFactory.getLogger(ResourceEventProducer::class.java)

    fun publishHarvestedEvents(
        dataType: DataType,
        resources: List<FdkIdAndUri>,
        resourceGraphs: Map<String, String>,
        runId: String
    ) {
        val topicName = getTopicForDataType(dataType)
        if (topicName == null) {
            logger.warn("No topic configured for dataType: $dataType")
            return
        }

        resources.forEach { resource ->
            try {
                val graph = resourceGraphs[resource.fdkId] ?: ""
                val event = createHarvestedEvent(dataType, resource, runId, graph)
                
                if (event != null) {
                    kafkaTemplate.send(topicName, resource.fdkId, event)
                    logProducedResourceEvent(topicName, resource.fdkId, "${dataType.name.uppercase()}_HARVESTED", resource.fdkId, resource.uri, runId, graph)
                }
            } catch (e: Exception) {
                logger.error("Failed to produce ${dataType.name.uppercase()}_HARVESTED event for fdkId: ${resource.fdkId}, runId: $runId", e)
            }
        }
    }

    fun publishRemovedEvents(
        dataType: DataType,
        resources: List<FdkIdAndUri>,
        runId: String
    ) {
        val topicName = getTopicForDataType(dataType)
        if (topicName == null) {
            logger.warn("No topic configured for dataType: $dataType")
            return
        }

        resources.forEach { resource ->
            try {
                // Removed events don't need graph content
                val event = createRemovedEvent(dataType, resource, runId)
                
                if (event != null) {
                    kafkaTemplate.send(topicName, resource.fdkId, event)
                    logProducedResourceEvent(topicName, resource.fdkId, "${dataType.name.uppercase()}_REMOVED", resource.fdkId, resource.uri, runId, "")
                }
            } catch (e: Exception) {
                logger.error("Failed to produce ${dataType.name.uppercase()}_REMOVED event for fdkId: ${resource.fdkId}, runId: $runId", e)
            }
        }
    }

    private fun createHarvestedEvent(
        dataType: DataType,
        resource: FdkIdAndUri,
        runId: String,
        graph: String
    ): SpecificRecord? {
        val timestamp = System.currentTimeMillis()
        
        return when (dataType) {
            DataType.dataset -> DatasetEvent.newBuilder()
                .setType(DatasetEventType.DATASET_HARVESTED)
                .setHarvestRunId(runId)
                .setUri(resource.uri)
                .setFdkId(resource.fdkId)
                .setGraph(graph)
                .setTimestamp(timestamp)
                .build()
            DataType.concept -> ConceptEvent(
                ConceptEventType.CONCEPT_HARVESTED,
                runId,
                resource.uri,
                resource.fdkId,
                graph,
                timestamp
            )
            DataType.dataservice -> DataServiceEvent(
                DataServiceEventType.DATA_SERVICE_HARVESTED,
                runId,
                resource.uri,
                resource.fdkId,
                graph,
                timestamp
            )
            DataType.informationmodel -> InformationModelEvent(
                InformationModelEventType.INFORMATION_MODEL_HARVESTED,
                runId,
                resource.uri,
                resource.fdkId,
                graph,
                timestamp
            )
            DataType.service, DataType.publicService -> ServiceEvent(
                ServiceEventType.SERVICE_HARVESTED,
                runId,
                resource.uri,
                resource.fdkId,
                graph,
                timestamp
            )
            DataType.event -> EventEvent(
                EventEventType.EVENT_HARVESTED,
                runId,
                resource.uri,
                resource.fdkId,
                graph,
                timestamp
            )
            else -> {
                logger.warn("Unsupported dataType for resource event: $dataType")
                null
            }
        }
    }

    private fun createRemovedEvent(
        dataType: DataType,
        resource: FdkIdAndUri,
        runId: String
    ): SpecificRecord? {
        val timestamp = System.currentTimeMillis()
        
        return when (dataType) {
            DataType.dataset -> DatasetEvent.newBuilder()
                .setType(DatasetEventType.DATASET_REMOVED)
                .setHarvestRunId(runId)
                .setUri(resource.uri)
                .setFdkId(resource.fdkId)
                .setGraph("")
                .setTimestamp(timestamp)
                .build()
            DataType.concept -> ConceptEvent(
                ConceptEventType.CONCEPT_REMOVED,
                runId,
                resource.uri,
                resource.fdkId,
                "", // Empty graph for removed events
                timestamp
            )
            DataType.dataservice -> DataServiceEvent(
                DataServiceEventType.DATA_SERVICE_REMOVED,
                runId,
                resource.uri,
                resource.fdkId,
                "", // Empty graph for removed events
                timestamp
            )
            DataType.informationmodel -> InformationModelEvent(
                InformationModelEventType.INFORMATION_MODEL_REMOVED,
                runId,
                resource.uri,
                resource.fdkId,
                "", // Empty graph for removed events
                timestamp
            )
            DataType.service, DataType.publicService -> ServiceEvent(
                ServiceEventType.SERVICE_REMOVED,
                runId,
                resource.uri,
                resource.fdkId,
                "", // Empty graph for removed events
                timestamp
            )
            DataType.event -> EventEvent(
                EventEventType.EVENT_REMOVED,
                runId,
                resource.uri,
                resource.fdkId,
                "", // Empty graph for removed events
                timestamp
            )
            else -> {
                logger.warn("Unsupported dataType for resource event: $dataType")
                null
            }
        }
    }

    private fun logProducedResourceEvent(topic: String, key: String, eventType: String, fdkId: String, uri: String, runId: String, graph: String) {
        if (!logger.isDebugEnabled) return
        logger.debug("Produced resource event: topic=$topic, key=$key, eventType=$eventType, fdkId=$fdkId, uri=$uri, runId=$runId, graph=$graph")
    }

    private fun getTopicForDataType(dataType: DataType): String? {
        return when (dataType) {
            DataType.dataset -> datasetEventsTopic
            DataType.concept -> conceptEventsTopic
            DataType.dataservice -> dataServiceEventsTopic
            DataType.informationmodel -> informationModelEventsTopic
            DataType.service, DataType.publicService -> serviceEventsTopic
            DataType.event -> eventEventsTopic
            else -> null
        }
    }
}

