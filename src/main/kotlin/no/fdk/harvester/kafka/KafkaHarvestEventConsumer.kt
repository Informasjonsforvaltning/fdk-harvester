package no.fdk.harvester.kafka

import no.fdk.harvest.HarvestEvent
import no.fdk.harvest.HarvestPhase
import no.fdk.harvester.metrics.KafkaHarvestMetrics
import no.fdk.harvester.metrics.KafkaHarvestMetrics.EventProcessingResult
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.Acknowledgment
import org.springframework.stereotype.Component
import java.time.Duration

/**
 * Consumes [HarvestEvent] records from the harvest-events topic. Processes only INITIATING phase
 * events by delegating to [KafkaHarvestEventCircuitBreaker]; other phases are skipped.
 */
@Component
class KafkaHarvestEventConsumer(
    private val circuitBreaker: KafkaHarvestEventCircuitBreakerApi,
) {
    private fun logger(): Logger = LOGGER

    @KafkaListener(
        topics = ["\${app.kafka.topic.harvest-events:harvest-events}"],
        groupId = "\${spring.kafka.consumer.group-id:fdk-harvester}",
        containerFactory = "kafkaListenerContainerFactory",
        id = HARVEST_LISTENER_ID,
    )
    fun consumeHarvestEvent(
        record: ConsumerRecord<String, HarvestEvent>,
        ack: Acknowledgment,
    ) {
        logger().debug("Received harvest event - offset: ${record.offset()}, partition: ${record.partition()}")

        val event = record.value()

        // Only process INITIATING phase events
        if (event.phase != HarvestPhase.INITIATING && event.phase != HarvestPhase.REMOVING) {
            logger().debug("Skipping harvest event with phase: {}", event.phase)
            KafkaHarvestMetrics.recordEventProcessed(event.phase, EventProcessingResult.SKIPPED)
            ack.acknowledge()
            return
        }

        logger().debug("Processing harvest event - phase: ${event.phase}, dataType: ${event.dataType}, dataSourceId: ${event.dataSourceId}")

        try {
            circuitBreaker.process(record)
            KafkaHarvestMetrics.recordEventProcessed(event.phase, EventProcessingResult.SUCCESS)
            ack.acknowledge()
        } catch (e: Exception) {
            logger().error("Error processing harvest event for dataSourceId: ${event.dataSourceId}, dataType: ${event.dataType}", e)
            KafkaHarvestMetrics.recordEventProcessed(event.phase, EventProcessingResult.ERROR)
            ack.nack(Duration.ZERO)
        }
    }

    companion object {
        private val LOGGER: Logger = LoggerFactory.getLogger(KafkaHarvestEventConsumer::class.java)
        const val HARVEST_LISTENER_ID = "harvest"
    }
}
