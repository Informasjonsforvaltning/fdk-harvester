package no.fdk.fdk_harvester.kafka

import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker
import no.fdk.fdk_harvester.kafka.HarvestEventProducer
import no.fdk.fdk_harvester.kafka.ResourceEventProducer
import no.fdk.fdk_harvester.service.HarvestServiceApi
import no.fdk.harvest.HarvestEvent
import no.fdk.harvest.HarvestPhase
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.time.Instant

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
) : KafkaHarvestEventCircuitBreakerApi {

    @CircuitBreaker(name = CIRCUIT_BREAKER_ID)
    override fun process(record: ConsumerRecord<String, HarvestEvent>) {
        val event = record.value()
        if (event.phase != HarvestPhase.INITIATING) {
            LOGGER.debug("Ignoring harvest event with phase: ${event.phase} (only INITIATING is processed)")
            return
        }

        LOGGER.debug("Processing harvest event - offset: ${record.offset()}, partition: ${record.partition()}")

        try {
            val runId = requireNotNull(event.runId?.toString()) { "Harvest event missing runId" }
            val dataSourceId = requireNotNull(event.dataSourceId?.toString()) { 
                "Harvest event missing dataSourceId" 
            }
            val dataSourceUrl = requireNotNull(event.dataSourceUrl?.toString()) { 
                "Harvest event missing dataSourceUrl" 
            }
            
            if (event.removeAll == true) {
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
            } else {
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
            }

            LOGGER.debug("Successfully processed harvest event for dataSourceId: ${event.dataSourceId}, dataType: ${event.dataType}")
        } catch (e: IllegalArgumentException) {
            LOGGER.error("${e.message}, skipping")
        } catch (e: Exception) {
            LOGGER.error("Error processing harvest event for dataSourceId: ${event.dataSourceId}, dataType: ${event.dataType}", e)
            throw e
        }
    }

    companion object {
        private val LOGGER: Logger = LoggerFactory.getLogger(KafkaHarvestEventCircuitBreaker::class.java)
        const val CIRCUIT_BREAKER_ID = "harvest-cb"
    }
}
