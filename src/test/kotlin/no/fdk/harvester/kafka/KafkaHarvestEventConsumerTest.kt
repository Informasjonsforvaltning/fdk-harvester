package no.fdk.harvester.kafka

import io.github.resilience4j.circuitbreaker.CallNotPermittedException
import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.micrometer.core.instrument.Metrics
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import no.fdk.harvest.DataType
import no.fdk.harvest.HarvestEvent
import no.fdk.harvest.HarvestPhase
import no.fdk.harvester.metrics.HarvestMetrics
import no.fdk.harvester.metrics.KafkaHarvestMetrics
import no.fdk.harvester.metrics.ResourceEventMetrics
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.springframework.kafka.support.Acknowledgment
import java.time.Duration

@Tag("unit")
class KafkaHarvestEventConsumerTest {
    private val circuitBreaker: KafkaHarvestEventCircuitBreakerApi = mockk()
    private val consumer = KafkaHarvestEventConsumer(circuitBreaker)
    private val ack: Acknowledgment = mockk(relaxed = true)
    private lateinit var meterRegistry: SimpleMeterRegistry

    @BeforeEach
    fun setUpMetrics() {
        meterRegistry = SimpleMeterRegistry()
        Metrics.addRegistry(meterRegistry)
        HarvestMetrics.bind(Metrics.globalRegistry)
        KafkaHarvestMetrics.bind(Metrics.globalRegistry)
        ResourceEventMetrics.bind(Metrics.globalRegistry)
    }

    @AfterEach
    fun tearDownMetrics() {
        Metrics.removeRegistry(meterRegistry)
        meterRegistry.clear()
    }

    @Test
    fun `consumer has non-null logger so logging never throws NPE`() {
        val loggerMethod = KafkaHarvestEventConsumer::class.java.getDeclaredMethod("logger")
        loggerMethod.isAccessible = true
        assertThat(loggerMethod.invoke(consumer)).isNotNull()
    }

    @Test
    fun `consumeHarvestEvent skips non-initiating phases and acknowledges`() {
        val event =
            HarvestEvent
                .newBuilder()
                .setPhase(HarvestPhase.HARVESTING)
                .setRunId("run-1")
                .setDataType(DataType.dataset)
                .setDataSourceId("source-1")
                .setDataSourceUrl("http://example.org/source")
                .setAcceptHeader("text/turtle")
                .setForced(false)
                .build()
        val record = ConsumerRecord("harvest-events", 0, 0L, "key", event)

        consumer.consumeHarvestEvent(record, ack)

        verify(exactly = 1) { ack.acknowledge() }
        verify(exactly = 0) { circuitBreaker.process(any()) }
        verify(exactly = 0) { ack.nack(any<Duration>()) }
        assertEquals(
            1.0,
            meterRegistry
                .counter(
                    "harvest_event_processing_total",
                    "phase",
                    "harvesting",
                    "result",
                    "skipped",
                ).count(),
        )
    }

    @Test
    fun `consumeHarvestEvent processes initiating phase and acknowledges on success`() {
        val event =
            HarvestEvent
                .newBuilder()
                .setPhase(HarvestPhase.INITIATING)
                .setRunId("run-1")
                .setDataType(DataType.dataset)
                .setDataSourceId("source-1")
                .setDataSourceUrl("http://example.org/source")
                .setAcceptHeader("text/turtle")
                .setForced(false)
                .build()
        val record = ConsumerRecord("harvest-events", 0, 0L, "key", event)

        every { circuitBreaker.process(any()) } returns Unit

        consumer.consumeHarvestEvent(record, ack)

        verify(exactly = 1) { circuitBreaker.process(record) }
        verify(exactly = 1) { ack.acknowledge() }
        verify(exactly = 0) { ack.nack(any<Duration>()) }
    }

    @Test
    fun `consumeHarvestEvent nacks on circuit breaker open`() {
        val event =
            HarvestEvent
                .newBuilder()
                .setPhase(HarvestPhase.INITIATING)
                .setRunId("run-1")
                .setDataType(DataType.dataset)
                .setDataSourceId("source-1")
                .setDataSourceUrl("http://example.org/source")
                .setAcceptHeader("text/turtle")
                .setForced(false)
                .build()
        val record = ConsumerRecord("harvest-events", 0, 0L, "key", event)

        every { circuitBreaker.process(any()) } throws
            CallNotPermittedException.createCallNotPermittedException(CircuitBreaker.ofDefaults("test"))

        consumer.consumeHarvestEvent(record, ack)

        verify(exactly = 1) { circuitBreaker.process(record) }
        verify(exactly = 1) { ack.nack(Duration.ZERO) }
        verify(exactly = 0) { ack.acknowledge() }
        assertEquals(
            1.0,
            meterRegistry
                .counter(
                    "harvest_event_processing_total",
                    "phase",
                    "initiating",
                    "result",
                    "circuit_open",
                ).count(),
        )
    }

    @Test
    fun `consumeHarvestEvent nacks on processing error`() {
        val event =
            HarvestEvent
                .newBuilder()
                .setPhase(HarvestPhase.INITIATING)
                .setRunId("run-1")
                .setDataType(DataType.dataset)
                .setDataSourceId("source-1")
                .setDataSourceUrl("http://example.org/source")
                .setAcceptHeader("text/turtle")
                .setForced(false)
                .build()
        val record = ConsumerRecord("harvest-events", 0, 0L, "key", event)

        every { circuitBreaker.process(any()) } throws RuntimeException("boom")

        consumer.consumeHarvestEvent(record, ack)

        verify(exactly = 1) { circuitBreaker.process(record) }
        verify(exactly = 1) { ack.nack(Duration.ZERO) }
        verify(exactly = 0) { ack.acknowledge() }
        assertEquals(
            1.0,
            meterRegistry
                .counter(
                    "harvest_event_processing_total",
                    "phase",
                    "initiating",
                    "result",
                    "nacked",
                ).count(),
        )
    }
}
