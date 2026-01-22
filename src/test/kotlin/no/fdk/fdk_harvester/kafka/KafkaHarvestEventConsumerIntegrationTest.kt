package no.fdk.fdk_harvester.kafka

import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import no.fdk.fdk_harvester.Application
import no.fdk.harvest.DataType
import no.fdk.harvest.HarvestEvent
import no.fdk.harvest.HarvestPhase
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Primary
import org.springframework.kafka.support.Acknowledgment
import org.springframework.test.context.ContextConfiguration
import org.springframework.test.context.TestPropertySource

/**
 * Integration test that runs [KafkaHarvestEventConsumer] as a Spring-managed bean (possibly proxied).
 * Ensures the consumer's logger is initialized so logging never throws NPE in production.
 */
@Tag("unit")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, classes = [Application::class])
@ContextConfiguration(classes = [KafkaHarvestEventConsumerIntegrationTest.MockCircuitBreakerConfig::class])
@TestPropertySource(
    properties = [
        "spring.kafka.listener.auto-startup=false",
        "spring.kafka.bootstrap-servers=localhost:9092",
        "spring.kafka.consumer.properties.schema.registry.url=http://localhost:8081",
        "spring.jpa.hibernate.ddl-auto=create-drop",
        "spring.flyway.enabled=false",
        "spring.datasource.url=jdbc:h2:mem:fdk_harvester_test;MODE=PostgreSQL;DB_CLOSE_DELAY=-1;DATABASE_TO_LOWER=TRUE",
        "spring.datasource.driver-class-name=org.h2.Driver",
        "spring.datasource.username=sa",
        "spring.datasource.password=",
        "spring.jpa.properties.hibernate.dialect=org.hibernate.dialect.H2Dialect",
    ]
)
class KafkaHarvestEventConsumerIntegrationTest {

    @Autowired
    private lateinit var consumer: KafkaHarvestEventConsumer

    @Autowired
    private lateinit var circuitBreaker: KafkaHarvestEventCircuitBreakerApi

    private val ack: Acknowledgment = mockk(relaxed = true)

    @Test
    fun `Spring-managed consumer can process event without NPE from logger`() {
        val event = HarvestEvent.newBuilder()
            .setPhase(HarvestPhase.INITIATING)
            .setRunId("run-1")
            .setDataType(DataType.dataset)
            .setDataSourceId("source-1")
            .setDataSourceUrl("http://example.org/source")
            .setAcceptHeader("text/turtle")
            .setForced(false)
            .build()
        val record = ConsumerRecord<String, HarvestEvent>("harvest-events", 0, 0L, "key", event)

        every { circuitBreaker.process(any()) } returns Unit

        consumer.consumeHarvestEvent(record, ack)

        verify(exactly = 1) { circuitBreaker.process(record) }
        verify(exactly = 1) { ack.acknowledge() }
    }

    @Configuration
    open class MockCircuitBreakerConfig {
        @Bean
        @Primary
        open fun circuitBreaker(): KafkaHarvestEventCircuitBreakerApi = mockk(relaxed = true)
    }
}
