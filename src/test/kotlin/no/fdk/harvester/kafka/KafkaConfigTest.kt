package no.fdk.harvester.kafka

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.springframework.kafka.listener.ContainerProperties

@Tag("unit")
class KafkaConfigTest {
    private fun config(maxPollRecords: Int = 1, maxPollIntervalMs: Int = 1_800_000) = KafkaConfig(
        bootstrapServers = "localhost:9092",
        securityProtocol = "PLAINTEXT",
        saslMechanism = "SCRAM-SHA-512",
        saslJaasConfig = "",
        schemaRegistryUrl = "http://localhost:8081",
        maxPollRecords = maxPollRecords,
        maxPollIntervalMs = maxPollIntervalMs,
    )

    @Test
    fun `listener container factory uses MANUAL_IMMEDIATE ack mode so each event is committed on acknowledge`() {
        val factory = config().kafkaListenerContainerFactory()

        assertThat(factory.containerProperties.ackMode)
            .isEqualTo(ContainerProperties.AckMode.MANUAL_IMMEDIATE)
    }

    @Test
    fun `consumer factory limits poll size and allows long processing between polls`() {
        val props = config().consumerFactory().configurationProperties

        assertThat(props[ConsumerConfig.MAX_POLL_RECORDS_CONFIG]).isEqualTo(1)
        assertThat(props[ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG]).isEqualTo(1_800_000)
    }

    @Test
    fun `consumer factory uses configured poll settings`() {
        val props = config(maxPollRecords = 3, maxPollIntervalMs = 900_000).consumerFactory().configurationProperties

        assertThat(props[ConsumerConfig.MAX_POLL_RECORDS_CONFIG]).isEqualTo(3)
        assertThat(props[ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG]).isEqualTo(900_000)
    }
}
