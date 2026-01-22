package no.fdk.fdk_harvester.service

import no.fdk.fdk_harvester.Application
import no.fdk.fdk_harvester.model.HarvestSourceEntity
import no.fdk.fdk_harvester.model.ResourceEntity
import no.fdk.fdk_harvester.model.ResourceType
import no.fdk.fdk_harvester.repository.HarvestSourceRepository
import no.fdk.fdk_harvester.repository.ResourceRepository
import no.fdk.harvest.DataType
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.springframework.aop.support.AopUtils
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.TestPropertySource
import java.time.Instant

/**
 * Regression test for Boot 4 / Spring 7 AOP proxying.
 *
 * Ensures we call HarvestService through a JDK proxy (interface-based) and that constructor-injected
 * dependencies are present (not null) when invoked via the proxy.
 */
@Tag("unit")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, classes = [Application::class])
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

        // Ensure we use JDK proxies when interfaces exist (recommended for Boot 4).
        "spring.aop.proxy-target-class=false",
    ]
)
class HarvestServiceProxyIntegrationTest {

    @Autowired
    private lateinit var harvestService: HarvestServiceApi

    @Autowired
    private lateinit var harvestSourceRepository: HarvestSourceRepository

    @Autowired
    private lateinit var resourceRepository: ResourceRepository

    @Test
    fun `markResourcesAsDeleted works when called via JDK proxy`() {
        assertTrue(AopUtils.isAopProxy(harvestService))
        assertTrue(AopUtils.isJdkDynamicProxy(harvestService))

        val sourceUrl = "http://example.org/source"
        val now = Instant.now()

        val source = harvestSourceRepository.save(
            HarvestSourceEntity(
                uri = sourceUrl,
                checksum = "c",
                issued = now,
            )
        )
        resourceRepository.save(
            ResourceEntity(
                uri = "http://example.org/dataset1",
                type = ResourceType.DATASET,
                fdkId = "fdk-1",
                removed = false,
                issued = now,
                modified = now,
                checksum = "x",
                harvestSource = source,
            )
        )

        val report = harvestService.markResourcesAsDeleted(
            sourceUrl = sourceUrl,
            dataType = DataType.dataset,
            dataSourceId = "ds-1",
            runId = "run-1",
        )

        assertEquals(1, report.removedResources.size)
        assertEquals("fdk-1", report.removedResources[0].fdkId)

        val updated = resourceRepository.findById("http://example.org/dataset1").orElse(null)
        assertNotNull(updated)
        assertEquals(true, updated!!.removed)
    }
}

