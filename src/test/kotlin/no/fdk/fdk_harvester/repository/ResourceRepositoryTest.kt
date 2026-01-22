package no.fdk.fdk_harvester.repository

import no.fdk.fdk_harvester.Application
import no.fdk.fdk_harvester.model.HarvestSourceEntity
import no.fdk.fdk_harvester.model.ResourceEntity
import no.fdk.fdk_harvester.model.ResourceType
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
class ResourceRepositoryTest {

    @Autowired
    private lateinit var entityManager: EntityManager

    @Autowired
    private lateinit var resourceRepository: ResourceRepository

    @Test
    fun `test save and findById`() {
        val harvestSource = createHarvestSource("http://example.org/source")
        val resource = createResource("http://example.org/resource1", ResourceType.DATASET, harvestSource)

        entityManager.persist(harvestSource)
        entityManager.persist(resource)
        entityManager.flush()

        val found = resourceRepository.findById("http://example.org/resource1")
        assertTrue(found.isPresent)
        assertEquals("http://example.org/resource1", found.get().uri)
        assertEquals(ResourceType.DATASET, found.get().type)
    }

    @Test
    fun `test findAllByType`() {
        val harvestSource = createHarvestSource("http://example.org/source")
        entityManager.persist(harvestSource)

        val dataset1 = createResource("http://example.org/dataset1", ResourceType.DATASET, harvestSource)
        val dataset2 = createResource("http://example.org/dataset2", ResourceType.DATASET, harvestSource)
        val concept1 = createResource("http://example.org/concept1", ResourceType.CONCEPT, harvestSource)

        entityManager.persist(dataset1)
        entityManager.persist(dataset2)
        entityManager.persist(concept1)
        entityManager.flush()

        val datasets = resourceRepository.findAllByType(ResourceType.DATASET)
        assertEquals(2, datasets.size)
        assertTrue(datasets.all { it.type == ResourceType.DATASET })
    }

    @Test
    fun `test findAllByTypeAndRemoved`() {
        val harvestSource = createHarvestSource("http://example.org/source")
        entityManager.persist(harvestSource)

        val active = createResource("http://example.org/active", ResourceType.DATASET, harvestSource, removed = false)
        val removed = createResource("http://example.org/removed", ResourceType.DATASET, harvestSource, removed = true)

        entityManager.persist(active)
        entityManager.persist(removed)
        entityManager.flush()

        val activeResources = resourceRepository.findAllByTypeAndRemoved(ResourceType.DATASET, false)
        assertEquals(1, activeResources.size)
        assertEquals("http://example.org/active", activeResources[0].uri)

        val removedResources = resourceRepository.findAllByTypeAndRemoved(ResourceType.DATASET, true)
        assertEquals(1, removedResources.size)
        assertEquals("http://example.org/removed", removedResources[0].uri)
    }

    @Test
    fun `test findByFdkId`() {
        val harvestSource = createHarvestSource("http://example.org/source")
        entityManager.persist(harvestSource)

        val resource = createResource("http://example.org/resource", ResourceType.DATASET, harvestSource, fdkId = "fdk-123")
        entityManager.persist(resource)
        entityManager.flush()

        val found = resourceRepository.findByFdkId("fdk-123")
        assertNotNull(found)
        assertEquals("fdk-123", found?.fdkId)
    }

    @Test
    fun `test findAllByFdkId`() {
        val harvestSource = createHarvestSource("http://example.org/source")
        entityManager.persist(harvestSource)

        val resource1 = createResource("http://example.org/resource1", ResourceType.DATASET, harvestSource, fdkId = "fdk-123")
        val resource2 = createResource("http://example.org/resource2", ResourceType.CONCEPT, harvestSource, fdkId = "fdk-123")

        entityManager.persist(resource1)
        entityManager.persist(resource2)
        entityManager.flush()

        val found = resourceRepository.findAllByFdkId("fdk-123")
        assertEquals(2, found.size)
        assertTrue(found.all { it.fdkId == "fdk-123" })
    }

    private fun createHarvestSource(uri: String): HarvestSourceEntity {
        return HarvestSourceEntity(
            uri = uri,
            checksum = "checksum-123",
            issued = java.time.Instant.now()
        )
    }

    private fun createResource(
        uri: String,
        type: ResourceType,
        harvestSource: HarvestSourceEntity,
        fdkId: String = "fdk-$uri",
        removed: Boolean = false
    ): ResourceEntity {
        return ResourceEntity(
            uri = uri,
            type = type,
            fdkId = fdkId,
            removed = removed,
            issued = java.time.Instant.now(),
            modified = java.time.Instant.now(),
            checksum = "checksum-$uri",
            harvestSource = harvestSource
        )
    }
}

