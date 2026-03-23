package no.fdk.fdk_harvester.config

import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.mockk.mockk
import io.mockk.verify
import no.fdk.fdk_harvester.config.CircuitBreakerConsumerConfiguration.Companion.CIRCUIT_BREAKER_ID
import no.fdk.fdk_harvester.kafka.KafkaManager
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import java.time.Duration

@Tag("unit")
class CircuitBreakerConsumerConfigurationTest {

    @Test
    fun `circuit breaker opens after repeated failures and pauses kafka listener`() {
        val kafkaManager = mockk<KafkaManager>(relaxed = true)

        val cbConfig = CircuitBreakerConfig.custom()
            .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
            .slidingWindowSize(2)
            .minimumNumberOfCalls(2)
            .failureRateThreshold(50.0f)
            .waitDurationInOpenState(Duration.ofMillis(10))
            .build()

        val registry = CircuitBreakerRegistry.of(cbConfig)
        val cb = registry.circuitBreaker(CIRCUIT_BREAKER_ID)

        val config = CircuitBreakerConsumerConfiguration(kafkaManager)
        config.registerListeners(registry)

        repeat(2) {
            try {
                cb.executeSupplier<String> { throw RuntimeException("boom") }
            } catch (_: Exception) {
                // ignore
            }
        }

        assertEquals(io.github.resilience4j.circuitbreaker.CircuitBreaker.State.OPEN, cb.state)
        verify(exactly = 1) { kafkaManager.pause("harvest") }
    }

    @Test
    fun `circuit breaker half-open and closed resumes kafka listener`() {
        val kafkaManager = mockk<KafkaManager>(relaxed = true)

        val registry = CircuitBreakerRegistry.ofDefaults()
        val cb = registry.circuitBreaker(CIRCUIT_BREAKER_ID)

        val config = CircuitBreakerConsumerConfiguration(kafkaManager)
        config.registerListeners(registry)

        cb.transitionToOpenState()
        cb.transitionToHalfOpenState()
        cb.transitionToClosedState()

        // OPEN->HALF_OPEN triggers resume, HALF_OPEN->CLOSED triggers resume
        verify(atLeast = 1) { kafkaManager.resume("harvest") }
    }
}




