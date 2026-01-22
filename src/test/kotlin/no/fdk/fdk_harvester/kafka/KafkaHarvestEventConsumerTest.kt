package no.fdk.fdk_harvester.kafka

import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import no.fdk.harvest.DataType
import no.fdk.harvest.HarvestEvent
import no.fdk.harvest.HarvestPhase
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.springframework.kafka.support.Acknowledgment
import java.time.Duration

@Tag("unit")
class KafkaHarvestEventConsumerTest {

    private val circuitBreaker: KafkaHarvestEventCircuitBreakerApi = mockk()
    private val consumer = KafkaHarvestEventConsumer(circuitBreaker)
    private val ack: Acknowledgment = mockk(relaxed = true)

    @Test
    fun `consumer has non-null logger so logging never throws NPE`() {
        val loggerMethod = KafkaHarvestEventConsumer::class.java.getDeclaredMethod("logger")
        loggerMethod.isAccessible = true
        assertThat(loggerMethod.invoke(consumer)).isNotNull()
    }

    @Test
    fun `consumeHarvestEvent skips non-initiating phases and acknowledges`() {
        val event = HarvestEvent.newBuilder()
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
    }

    @Test
    fun `consumeHarvestEvent processes initiating phase and acknowledges on success`() {
        val event = HarvestEvent.newBuilder()
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
    fun `consumeHarvestEvent nacks on processing error`() {
        val event = HarvestEvent.newBuilder()
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
    }
}




