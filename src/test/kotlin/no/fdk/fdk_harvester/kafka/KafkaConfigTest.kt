package no.fdk.fdk_harvester.kafka

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.springframework.kafka.listener.ContainerProperties

@Tag("unit")
class KafkaConfigTest {

    @Test
    fun `listener container factory uses MANUAL ack mode so Acknowledgment is available to listeners`() {
        val config = KafkaConfig(
            bootstrapServers = "localhost:9092",
            schemaRegistryUrl = "http://localhost:8081",
        )
        val factory = config.kafkaListenerContainerFactory()

        assertThat(factory.containerProperties.ackMode)
            .isEqualTo(ContainerProperties.AckMode.MANUAL)
    }
}
