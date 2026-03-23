package no.fdk.fdk_harvester.config

import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.github.resilience4j.circuitbreaker.CircuitBreaker.StateTransition
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.github.resilience4j.circuitbreaker.event.CircuitBreakerOnStateTransitionEvent
import no.fdk.fdk_harvester.kafka.KafkaHarvestEventConsumer
import no.fdk.fdk_harvester.kafka.KafkaManager
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.time.Duration

/**
 * Listens to circuit breaker state transitions and pauses/resumes the harvest Kafka consumer
 * when the breaker opens (e.g. after repeated harvest failures) or closes again.
 */
@Configuration
open class CircuitBreakerConsumerConfiguration(
    private val kafkaManager: KafkaManager,
) {


    @Bean
    open fun circuitBreakerRegistry(): CircuitBreakerRegistry {
        val defaultConfig =
            CircuitBreakerConfig.custom()
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .slidingWindowSize(10)
                .minimumNumberOfCalls(5)
                .failureRateThreshold(50f)
                .waitDurationInOpenState(Duration.ofSeconds(30))
                .automaticTransitionFromOpenToHalfOpenEnabled(true)
                .build()

        val registry = CircuitBreakerRegistry.of(defaultConfig)
        registerListeners(registry)
        return registry
    }

    open fun registerListeners(registry: CircuitBreakerRegistry) {
        attachListener(registry)
    }

    private fun attachListener(registry: CircuitBreakerRegistry) {
        registry.circuitBreaker(CIRCUIT_BREAKER_ID)
            .eventPublisher
            .onStateTransition { event: CircuitBreakerOnStateTransitionEvent ->
                handleStateTransition(event)
            }
    }

    private fun handleStateTransition(event: CircuitBreakerOnStateTransitionEvent) {
        LOGGER.debug("Handling state transition in circuit breaker {}", event)
        when (event.stateTransition) {
            StateTransition.CLOSED_TO_OPEN,
            StateTransition.CLOSED_TO_FORCED_OPEN,
            StateTransition.HALF_OPEN_TO_OPEN,
            -> {
                LOGGER.warn("Circuit breaker opened, pausing Kafka listener: ${KafkaHarvestEventConsumer.HARVEST_LISTENER_ID}")
                kafkaManager.pause(KafkaHarvestEventConsumer.HARVEST_LISTENER_ID)
            }

            StateTransition.OPEN_TO_HALF_OPEN,
            StateTransition.HALF_OPEN_TO_CLOSED,
            StateTransition.FORCED_OPEN_TO_CLOSED,
            StateTransition.FORCED_OPEN_TO_HALF_OPEN,
            -> {
                LOGGER.info("Circuit breaker closed, resuming Kafka listener: ${KafkaHarvestEventConsumer.HARVEST_LISTENER_ID}")
                kafkaManager.resume(KafkaHarvestEventConsumer.HARVEST_LISTENER_ID)
            }

            else -> throw IllegalStateException("Unknown transition state: " + event.stateTransition)
        }
    }

    @Bean
    open fun harvesterCircuitBreaker(registry: CircuitBreakerRegistry): CircuitBreaker =
        registry.circuitBreaker(CIRCUIT_BREAKER_ID)

    companion object {
        private val LOGGER: Logger = LoggerFactory.getLogger(CircuitBreakerConsumerConfiguration::class.java)
        const val CIRCUIT_BREAKER_ID = "harvest-cb"
    }
}



