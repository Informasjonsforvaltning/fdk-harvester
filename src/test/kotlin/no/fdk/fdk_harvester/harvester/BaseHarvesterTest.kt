package no.fdk.fdk_harvester.harvester

import no.fdk.fdk_harvester.model.HarvestDataSource
import no.fdk.fdk_harvester.model.HarvestReport
import no.fdk.fdk_harvester.model.HarvestSourceEntity
import no.fdk.fdk_harvester.model.ResourceEntity
import no.fdk.fdk_harvester.repository.HarvestSourceRepository
import io.mockk.*
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import java.time.Instant
import java.util.*

@Tag("unit")
class BaseHarvesterTest {

    private val harvestSourceRepository: HarvestSourceRepository = mockk()

    private lateinit var testHarvester: TestHarvester

    @BeforeEach
    fun setUp() {
        clearAllMocks()
        testHarvester = TestHarvester(harvestSourceRepository)
    }

    @Test
    fun `BaseHarvester subclass has non-null logger so logging never throws NPE`() {
        val loggerGetter = BaseHarvester::class.java.getDeclaredMethod("getLogger")
        loggerGetter.isAccessible = true
        assertNotNull(loggerGetter.invoke(testHarvester))
    }

    @Test
    fun `test getOrCreateHarvestSource creates new when not exists`() {
        val uri = "http://example.org/source"
        val checksum = "abc123"
        val issued = Instant.now()

        every { harvestSourceRepository.findByUri(uri) } returns null
        every { harvestSourceRepository.save(any()) } answers { firstArg() }

        val result = testHarvester.testGetOrCreateHarvestSource(uri, checksum, issued)

        assertNotNull(result)
        assertEquals(uri, result.uri)
        assertEquals(checksum, result.checksum)
        assertEquals(issued, result.issued)
        verify(exactly = 1) { harvestSourceRepository.save(any<HarvestSourceEntity>()) }
    }

    @Test
    fun `test getOrCreateHarvestSource returns existing without updating`() {
        val uri = "http://example.org/source"
        val existing = HarvestSourceEntity(id = 1L, uri = uri, checksum = "old", issued = Instant.EPOCH)
        val newChecksum = "new123"
        val newIssued = Instant.now()

        every { harvestSourceRepository.findByUri(uri) } returns existing

        val result = testHarvester.testGetOrCreateHarvestSource(uri, newChecksum, newIssued)

        assertEquals(existing.checksum, result.checksum)
        assertEquals(existing.issued, result.issued)
        verify(exactly = 0) { harvestSourceRepository.save(any<HarvestSourceEntity>()) }
    }

    @Test
    fun `test validateSourceUrl passes when resource does not exist`() {
        val resourceUri = "http://example.org/resource"
        val harvestSource = HarvestSourceEntity(id = 1L, uri = "http://example.org/source", checksum = "abc", issued = Instant.now())

        assertDoesNotThrow {
            testHarvester.testValidateSourceUrl(resourceUri, harvestSource, null)
        }
    }

    @Test
    fun `test validateSourceUrl passes when resource is removed`() {
        val resourceUri = "http://example.org/resource"
        val harvestSource = HarvestSourceEntity(id = 1L, uri = "http://example.org/source", checksum = "abc", issued = Instant.now())
        val dbResource = ResourceEntity(
            uri = resourceUri,
            type = no.fdk.fdk_harvester.model.ResourceType.DATASET,
            fdkId = "fdk-1",
            removed = true,
            issued = Instant.now(),
            modified = Instant.now(),
            checksum = "checksum",
            harvestSource = HarvestSourceEntity(id = 2L, uri = "http://example.org/other", checksum = "def", issued = Instant.now())
        )

        assertDoesNotThrow {
            testHarvester.testValidateSourceUrl(resourceUri, harvestSource, dbResource)
        }
    }

    @Test
    fun `test validateSourceUrl passes when same harvest source`() {
        val resourceUri = "http://example.org/resource"
        val harvestSource = HarvestSourceEntity(id = 1L, uri = "http://example.org/source", checksum = "abc", issued = Instant.now())
        val dbResource = ResourceEntity(
            uri = resourceUri,
            type = no.fdk.fdk_harvester.model.ResourceType.DATASET,
            fdkId = "fdk-1",
            removed = false,
            issued = Instant.now(),
            modified = Instant.now(),
            checksum = "checksum",
            harvestSource = harvestSource
        )

        assertDoesNotThrow {
            testHarvester.testValidateSourceUrl(resourceUri, harvestSource, dbResource)
        }
    }

    @Test
    fun `test validateSourceUrl throws conflict when different harvest source`() {
        val resourceUri = "http://example.org/resource"
        val harvestSource = HarvestSourceEntity(id = 1L, uri = "http://example.org/source1", checksum = "abc", issued = Instant.now())
        val dbResource = ResourceEntity(
            uri = resourceUri,
            type = no.fdk.fdk_harvester.model.ResourceType.DATASET,
            fdkId = "fdk-1",
            removed = false,
            issued = Instant.now(),
            modified = Instant.now(),
            checksum = "checksum",
            harvestSource = HarvestSourceEntity(id = 2L, uri = "http://example.org/source2", checksum = "def", issued = Instant.now())
        )

        val exception = assertThrows(HarvestSourceConflictException::class.java) {
            testHarvester.testValidateSourceUrl(resourceUri, harvestSource, dbResource)
        }

        assertTrue(exception.message?.contains(resourceUri) == true)
        assertTrue(exception.message?.contains("already exists") == true)
    }

    // Test implementation of BaseHarvester for testing
    private class TestHarvester(
        harvestSourceRepository: HarvestSourceRepository
    ) : BaseHarvester(harvestSourceRepository) {
        
        override fun updateDB(
            harvested: org.apache.jena.rdf.model.Model,
            source: HarvestDataSource,
            harvestDate: Calendar,
            forceUpdate: Boolean,
            runId: String,
            dataType: String,
            harvestSource: HarvestSourceEntity
        ): HarvestReport {
            return HarvestReportBuilder.createSuccessReport(
                dataType = dataType,
                sourceId = source.id!!,
                sourceUrl = source.url!!,
                harvestDate = harvestDate,
                changedCatalogs = emptyList(),
                changedResources = emptyList(),
                removedResources = emptyList(),
                runId = runId
            )
        }
        
        // Expose protected methods for testing
        fun testGetOrCreateHarvestSource(uri: String, checksum: String, issued: Instant): HarvestSourceEntity {
            return getOrCreateHarvestSource(uri, checksum, issued)
        }
        
        fun testValidateSourceUrl(resourceUri: String, harvestSource: HarvestSourceEntity, dbResource: ResourceEntity?) {
            return validateSourceUrl(resourceUri, harvestSource, dbResource)
        }
    }
}

