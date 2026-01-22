package no.fdk.fdk_harvester.kafka

import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.springframework.kafka.config.KafkaListenerEndpointRegistry
import org.springframework.kafka.listener.MessageListenerContainer

@Tag("unit")
class KafkaManagerTest {

    @Test
    fun `pause and resume only targets matching listener id`() {
        val c1 = mockk<MessageListenerContainer>(relaxed = true)
        every { c1.listenerId } returns "harvest"

        val c2 = mockk<MessageListenerContainer>(relaxed = true)
        every { c2.listenerId } returns "other"

        val registry = mockk<KafkaListenerEndpointRegistry>()
        every { registry.listenerContainers } returns listOf(c1, c2)

        val manager = KafkaManager(registry)

        manager.pause("harvest")
        verify(exactly = 1) { c1.pause() }
        verify(exactly = 0) { c2.pause() }

        manager.resume("harvest")
        verify(exactly = 1) { c1.resume() }
        verify(exactly = 0) { c2.resume() }
    }
}




