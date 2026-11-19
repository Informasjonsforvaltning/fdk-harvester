package no.fdk.harvester.repository

import jakarta.persistence.EntityManager
import no.fdk.harvester.Application
import no.fdk.harvester.model.HarvestSourceEntity
import no.fdk.harvester.model.ResourceEntity
import no.fdk.harvester.model.ResourceType
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.TestPropertySource
import org.springframework.transaction.annotation.Transactional

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
        "spring.kafka.consumer.properties.schema.registry.url=http://localhost:8081",
    ],
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

    @Test
    fun `findSourcesByFdkIds returns source url for member resources that are not removed`() {
        val sourceA = createHarvestSource("http://example.org/source-a")
        val sourceB = createHarvestSource("http://example.org/source-b")
        entityManager.persist(sourceA)
        entityManager.persist(sourceB)
        entityManager.persist(createResource("http://example.org/a1", ResourceType.DATASET, sourceA, fdkId = "a1"))
        entityManager.persist(createResource("http://example.org/b1", ResourceType.DATASET, sourceB, fdkId = "b1"))
        entityManager.persist(createResource("http://example.org/b2", ResourceType.DATASET, sourceB, fdkId = "b2", removed = true))
        entityManager.persist(createResource("http://example.org/c1", ResourceType.DATASET, sourceB, fdkId = "c1"))
        entityManager.persist(createResource("http://example.org/catalog", ResourceType.CATALOG, sourceB, fdkId = "catalog"))
        entityManager.flush()

        val result = resourceRepository.findSourcesByFdkIds(listOf("a1", "b1", "b2", "catalog", "unknown"))

        assertEquals(
            setOf("a1" to "http://example.org/source-a", "b1" to "http://example.org/source-b"),
            result.map { it.fdkId to it.sourceUrl }.toSet(),
        )
        assertTrue(result.all { it.type == ResourceType.DATASET })
    }

    @Test
    fun `findSourcesByFdkIds returns each resource type from the same source`() {
        val source = createHarvestSource("http://example.org/source")
        entityManager.persist(source)
        entityManager.persist(createResource("http://example.org/dataset", ResourceType.DATASET, source, fdkId = "dataset"))
        entityManager.persist(createResource("http://example.org/dataservice", ResourceType.DATASERVICE, source, fdkId = "dataservice"))
        entityManager.flush()

        val result = resourceRepository.findSourcesByFdkIds(listOf("dataset", "dataservice"))

        assertEquals(
            setOf(
                Triple("dataset", ResourceType.DATASET, "http://example.org/source"),
                Triple("dataservice", ResourceType.DATASERVICE, "http://example.org/source"),
            ),
            result.map { Triple(it.fdkId, it.type, it.sourceUrl) }.toSet(),
        )
    }

    private fun createHarvestSource(uri: String): HarvestSourceEntity = HarvestSourceEntity(
        uri = uri,
        checksum = "checksum-123",
        issued = java.time.Instant.now(),
    )

    private fun createResource(
        uri: String,
        type: ResourceType,
        harvestSource: HarvestSourceEntity,
        fdkId: String = "fdk-$uri",
        removed: Boolean = false,
    ): ResourceEntity = ResourceEntity(
        uri = uri,
        type = type,
        fdkId = fdkId,
        removed = removed,
        issued = java.time.Instant.now(),
        modified = java.time.Instant.now(),
        checksum = "checksum-$uri",
        harvestSource = harvestSource,
    )
}
