package no.fdk.fdk_harvester.kafka

import no.fdk.fdk_harvester.model.HarvestReport
import no.fdk.harvest.DataType
import no.fdk.harvest.HarvestEvent
import no.fdk.harvest.HarvestPhase
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Service
import java.util.concurrent.CompletableFuture

/**
 * Produces [HarvestEvent] messages to the harvest-events topic (e.g. HARVESTING phase with report counts).
 */
@Service
class HarvestEventProducer(
    private val kafkaTemplate: KafkaTemplate<String, HarvestEvent>,
    @param:Value("\${app.kafka.topic.harvest-events:harvest-events}") private val topicName: String
) {
    private val logger = LoggerFactory.getLogger(HarvestEventProducer::class.java)

    /**
     * Builds a HARVESTING phase event from an initiating event and optional report, and sends it to the topic.
     * @return the send future, or null if runId is missing or send fails
     */
    fun produceHarvestingEvent(initiatingEvent: HarvestEvent, report: HarvestReport?): CompletableFuture<*>? {
        return try {
            if(initiatingEvent.runId == null) {
                logger.error("Initiating event is missing runId, cannot produce harvesting event")
                return null
            }

            val harvestingEvent = HarvestEvent.newBuilder(initiatingEvent)
                .setPhase(HarvestPhase.HARVESTING)
                .setChangedResourcesCount(report?.changedResources?.size)
                .setRemovedResourcesCount(report?.removedResources?.size)
                .setStartTime(report?.startTime)
                .setEndTime(report?.endTime)
                .setErrorMessage(report?.errorMessage)
                .build()

            val key = initiatingEvent.runId.toString()
            val future = kafkaTemplate.send(topicName, key, harvestingEvent)
            logger.info(
                "Produced harvest event: topic={}, key={}, phase={}, runId={}, dataType={}, dataSourceId={}, dataSourceUrl={}, startTime={}, endTime={}, errorMessage={}, changedResourcesCount={}, removedResourcesCount={}",
                topicName, key, harvestingEvent.phase, harvestingEvent.runId, harvestingEvent.dataType,
                harvestingEvent.dataSourceId, harvestingEvent.dataSourceUrl, harvestingEvent.startTime, harvestingEvent.endTime,
                harvestingEvent.errorMessage, harvestingEvent.changedResourcesCount, harvestingEvent.removedResourcesCount
            )
            future
        } catch (ex: Exception) {
            logger.error("Failed to produce harvesting event", ex)
            null
        }
    }

    /**
     * Sends a HARVESTING phase event with the given run and report metadata. No-op if runId is null.
     */
    fun produceHarvestingEvent(
        runId: String,
        dataType: DataType,
        dataSourceId: String,
        dataSourceUrl: String,
        startTime: String,
        endTime: String,
        errorMessage: String?,
        changedResourcesCount: Int,
        removedResourcesCount: Int
    ) {
        if (runId == null) {
            logger.debug("Skipping harvest event production - no runId")
            return
        }

        try {
            val event = HarvestEvent.newBuilder()
                .setPhase(HarvestPhase.HARVESTING)
                .setRunId(runId)
                .setDataType(dataType)
                .setDataSourceId(dataSourceId)
                .setDataSourceUrl(dataSourceUrl)
                .setStartTime(startTime)
                .setEndTime(endTime)
                .setErrorMessage(errorMessage)
                .setFdkId(null)
                .setResourceUri(null)
                .setAcceptHeader(null)
                .setChangedResourcesCount(changedResourcesCount)
                .setRemovedResourcesCount(removedResourcesCount)
                .build()

            kafkaTemplate.send(topicName, runId, event)
            logger.info(
                "Produced harvest event: topic={}, key={}, phase={}, runId={}, dataType={}, dataSourceId={}, dataSourceUrl={}, startTime={}, endTime={}, errorMessage={}, changedResourcesCount={}, removedResourcesCount={}",
                topicName, runId, event.phase, event.runId, event.dataType, event.dataSourceId, event.dataSourceUrl,
                event.startTime, event.endTime, event.errorMessage, event.changedResourcesCount, event.removedResourcesCount
            )
        } catch (e: Exception) {
            logger.error("Failed to produce HARVESTING event for runId: $runId, dataType: $dataType", e)
        }
    }
}


