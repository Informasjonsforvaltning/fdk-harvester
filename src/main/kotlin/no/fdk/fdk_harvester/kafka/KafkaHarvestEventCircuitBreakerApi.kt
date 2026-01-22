package no.fdk.fdk_harvester.kafka

import no.fdk.harvest.HarvestEvent
import org.apache.kafka.clients.consumer.ConsumerRecord

/**
 * Interface to allow JDK dynamic proxies for AOP (Resilience4j circuit breaker aspect).
 */
interface KafkaHarvestEventCircuitBreakerApi {
    fun process(record: ConsumerRecord<String, HarvestEvent>)
}

