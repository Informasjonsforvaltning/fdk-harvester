package no.fdk.fdk_harvester.kafka

import io.github.resilience4j.circuitbreaker.CircuitBreaker
import no.fdk.fdk_harvester.error.HarvestErrorCategory
import no.fdk.fdk_harvester.error.HarvestErrorMessageMapper
import no.fdk.fdk_harvester.service.HarvestServiceApi
import no.fdk.harvest.HarvestEvent
import no.fdk.harvest.HarvestPhase
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Component

/**
 * Processes only INITIATING phase [HarvestEvent] messages: either marks all resources for a source as deleted
 * (when [HarvestEvent.removeAll] is true) or runs a full harvest via [HarvestService], then publishes resource
 * events and HARVESTING phase events. All other phases are ignored.
 * Wrapped with Resilience4j circuit breaker to stop consuming on repeated failures.
 */
@Component
open class KafkaHarvestEventCircuitBreaker(
    private val harvestService: HarvestServiceApi,
    private val harvestEventProducer: HarvestEventProducer,
    private val resourceEventProducer: ResourceEventProducer,
    @param:Qualifier("harvesterCircuitBreaker")
    private val circuitBreaker: CircuitBreaker,
) : KafkaHarvestEventCircuitBreakerApi {

    override fun process(record: ConsumerRecord<String, HarvestEvent>) {
        circuitBreaker.executeRunnable {
            val event = record.value()

            LOGGER.debug("Processing harvest event - offset: ${record.offset()}, partition: ${record.partition()}")

            try {
                when (event.phase) {
                    HarvestPhase.INITIATING if event.removeAll != true -> initiateHarvest(event)
                    HarvestPhase.INITIATING if event.removeAll == true -> removeAllResourcesFromSource(event)
                    HarvestPhase.REMOVING -> removeSingleResource(event)
                    else -> {
                        LOGGER.debug(
                            "Ignoring harvest event with phase: {} (only INITIATING & REMOVING is processed)",
                            event.phase
                        )
                    }
                }
            } catch (e: IllegalArgumentException) {
                LOGGER.error("${e.message}, skipping")
                // Validation problem with the incoming event – report as a user-friendly failure.
                val errorMessage = HarvestErrorMessageMapper.toUserMessage(
                    category = HarvestErrorCategory.VALIDATION_ERROR,
                    dataSourceUrl = event.dataSourceUrl?.toString(),
                    dataType = event.dataType,
                )
                // We do not have a HarvestReport here, so emit an event directly with the mapped message.
                event.runId?.toString()?.let { runId ->
                    harvestEventProducer.produceHarvestingEvent(
                        runId = runId,
                        dataType = event.dataType,
                        dataSourceId = dataSourceIdOrNull(event),
                        dataSourceUrl = event.dataSourceUrl?.toString() ?: "",
                        startTime = null,
                        endTime = null,
                        errorMessage = errorMessage,
                        changedResourcesCount = 0,
                        removedResourcesCount = 0
                    )
                }
            } catch (e: Exception) {
                LOGGER.error(
                    "Error processing harvest event for dataSourceId: ${event.dataSourceId}, dataType: ${event.dataType}",
                    e
                )

                val category = when (e) {
                    is IllegalStateException -> HarvestErrorCategory.SOURCE_NOT_FOUND
                    else -> HarvestErrorCategory.INTERNAL_ERROR
                }

                val errorMessage = HarvestErrorMessageMapper.toUserMessage(
                    category = category,
                    dataSourceUrl = event.dataSourceUrl?.toString(),
                    dataType = event.dataType,
                )

                event.runId?.toString()?.let { runId ->
                    harvestEventProducer.produceHarvestingEvent(
                        runId = runId,
                        dataType = event.dataType,
                        dataSourceId = dataSourceIdOrNull(event),
                        dataSourceUrl = event.dataSourceUrl?.toString() ?: "",
                        startTime = null,
                        endTime = null,
                        errorMessage = errorMessage,
                        changedResourcesCount = 0,
                        removedResourcesCount = 0
                    )
                }

                throw e
            }
        }
    }

    private fun initiateHarvest(event: HarvestEvent) {
        val runId = requireNotNull(event.runId?.toString()) { "Harvest event missing runId" }
        val dataSourceId = requireNotNull(event.dataSourceId?.toString()) {
            "Harvest event missing dataSourceId"
        }
        val dataSourceUrl = requireNotNull(event.dataSourceUrl?.toString()) {
            "Harvest event missing dataSourceUrl"
        }

        val acceptHeader = requireNotNull(event.acceptHeader?.toString()) {
            "Harvest event missing acceptHeader"
        }

        val report = harvestService.executeHarvest(
            dataSourceId = dataSourceId,
            dataSourceUrl = dataSourceUrl,
            dataType = event.dataType,
            acceptHeader = acceptHeader,
            runId = runId,
            forced = event.forced ?: false
        )

        // Emit HARVESTING phase event after completion so counts are included.
        harvestEventProducer.produceHarvestingEvent(event, report)
        LOGGER.debug(
            "Successfully processed harvest event for dataSourceId: {}, dataType: {}",
            event.dataSourceId,
            event.dataType
        )

    }

    private fun removeAllResourcesFromSource(event: HarvestEvent) {
        val runId = requireNotNull(event.runId?.toString()) { "RemoveAll event missing runId" }
        val dataSourceId = requireNotNull(event.dataSourceId?.toString()) {
            "RemoveAll event missing dataSourceId"
        }
        val dataSourceUrl = requireNotNull(event.dataSourceUrl?.toString()) {
            "RemoveAll event missing dataSourceUrl"
        }

        val report = harvestService.markResourcesAsDeleted(
            sourceUrl = dataSourceUrl,
            dataType = event.dataType,
            dataSourceId = dataSourceId,
            runId = runId
        )

        LOGGER.info("Successfully marked ${report.removedResources.size} resources as deleted for dataSourceUrl: $dataSourceUrl")

        // Publish removed resource events
        if (report.removedResources.isNotEmpty()) {
            resourceEventProducer.publishRemovedEvents(
                dataType = event.dataType,
                resources = report.removedResources,
                runId = runId
            )
        }

        // Emit HARVESTING phase event after completion so counts are included.
        harvestEventProducer.produceHarvestingEvent(event, report)

        LOGGER.debug(
            "Successfully processed removeAll event for dataSourceId: {}, dataType: {}",
            event.dataSourceId,
            event.dataType
        )
    }

    private fun removeSingleResource(event: HarvestEvent) {
        val runId = requireNotNull(event.runId?.toString()) { "Remove event missing runId" }
        val fdkId = requireNotNull(event.fdkId?.toString()) { "Remove event missing fdkId" }
        val uri = requireNotNull(event.fdkId?.toString()) { "Remove event missing uri" }
        val dataSourceId = requireNotNull(event.dataSourceId?.toString()) {
            "Remove event missing dataSourceId"
        }

        val report = harvestService.markResourceAsDeletedByFdkId(
            fdkId = fdkId,
            uri = uri,
            dataType = event.dataType,
            runId = runId,
            dataSourceId = dataSourceId
        )

        // Publish removed resource event
        resourceEventProducer.publishRemovedEvents(
            dataType = event.dataType,
            resources = report.removedResources,
            runId = runId
        )

        // Emit HARVESTING phase event after completion so counts are included.
        harvestEventProducer.produceHarvestingEvent(event, report)

    }

    private fun dataSourceIdOrNull(event: HarvestEvent): String =
        event.dataSourceId?.toString() ?: ""

    companion object {
        private val LOGGER: Logger = LoggerFactory.getLogger(KafkaHarvestEventCircuitBreaker::class.java)
    }
}
