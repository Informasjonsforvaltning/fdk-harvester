package no.fdk.harvester.config

import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.github.resilience4j.micrometer.tagged.TaggedCircuitBreakerMetrics
import io.micrometer.core.instrument.MeterRegistry
import jakarta.annotation.PostConstruct
import no.fdk.harvester.metrics.HarvestMetrics
import no.fdk.harvester.metrics.KafkaHarvestMetrics
import no.fdk.harvester.metrics.ResourceEventMetrics
import org.springframework.context.annotation.Configuration

/** Binds Resilience4j circuit breaker metrics and custom harvest metrics to the application [MeterRegistry]. */
@Configuration
open class MetricsConfiguration(
    private val circuitBreakerRegistry: CircuitBreakerRegistry,
    private val meterRegistry: MeterRegistry,
) {
    @PostConstruct
    fun bindMetrics() {
        HarvestMetrics.bind(meterRegistry)
        KafkaHarvestMetrics.bind(meterRegistry)
        ResourceEventMetrics.bind(meterRegistry)

        TaggedCircuitBreakerMetrics
            .ofCircuitBreakerRegistry(circuitBreakerRegistry)
            .bindTo(meterRegistry)
    }
}
