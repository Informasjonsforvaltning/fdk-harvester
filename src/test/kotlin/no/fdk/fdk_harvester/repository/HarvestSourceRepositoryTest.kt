package no.fdk.fdk_harvester.repository

import no.fdk.fdk_harvester.Application
import no.fdk.fdk_harvester.model.HarvestSourceEntity
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.TestPropertySource
import org.springframework.transaction.annotation.Transactional
import jakarta.persistence.EntityManager

@Tag("unit")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, classes = [Application::class])
@Transactional
@TestPropertySource(
    properties = [
        "spring.jpa.hibernate.ddl-auto=create-drop",
        "spring.flyway.enabled=false",
        "spring.datasource.url=jdbc:h2:mem:fdk_harvester_test;MODE=PostgreSQL;DB_CLOSE_DELAY=-1;DATABASE_TO_LOWER=TRUE",
        "spring.datasource.driver-class-name=org.h2.Driver",
        "spring.datasource.username=sa",
        "spring.datasource.password=",
        "spring.jpa.properties.hibernate.dialect=org.hibernate.dialect.H2Dialect",
        "spring.kafka.listener.auto-startup=false",
        "spring.kafka.bootstrap-servers=localhost:9092",
        "spring.kafka.consumer.properties.schema.registry.url=http://localhost:8081"
    ]
)
class HarvestSourceRepositoryTest {

    @Autowired
    private lateinit var entityManager: EntityManager

    @Autowired
    private lateinit var harvestSourceRepository: HarvestSourceRepository

    @Test
    fun `test save and findById`() {
        val source = HarvestSourceEntity(
            uri = "http://example.org/source",
            checksum = "checksum-123",
            issued = java.time.Instant.now()
        )

        entityManager.persist(source)
        entityManager.flush()

        val found = harvestSourceRepository.findById(source.id!!)
        assertTrue(found.isPresent)
        assertEquals("http://example.org/source", found.get().uri)
        assertEquals("checksum-123", found.get().checksum)
    }

    @Test
    fun `test findByUri`() {
        val source1 = HarvestSourceEntity(
            uri = "http://example.org/source1",
            checksum = "checksum-1",
            issued = java.time.Instant.now()
        )
        val source2 = HarvestSourceEntity(
            uri = "http://example.org/source2",
            checksum = "checksum-2",
            issued = java.time.Instant.now()
        )

        entityManager.persist(source1)
        entityManager.persist(source2)
        entityManager.flush()

        val found = harvestSourceRepository.findByUri("http://example.org/source1")
        assertNotNull(found)
        assertEquals("http://example.org/source1", found?.uri)
        assertEquals("checksum-1", found?.checksum)
    }

    @Test
    fun `test findByUri returns null when not found`() {
        val found = harvestSourceRepository.findByUri("http://example.org/nonexistent")
        assertNull(found)
    }

    @Test
    fun `test unique constraint on uri`() {
        val source1 = HarvestSourceEntity(
            uri = "http://example.org/source",
            checksum = "checksum-1",
            issued = java.time.Instant.now()
        )

        entityManager.persist(source1)
        entityManager.flush()

        val source2 = HarvestSourceEntity(
            uri = "http://example.org/source",
            checksum = "checksum-2",
            issued = java.time.Instant.now()
        )

        assertThrows(Exception::class.java) {
            entityManager.persist(source2)
            entityManager.flush()
        }
    }
}

