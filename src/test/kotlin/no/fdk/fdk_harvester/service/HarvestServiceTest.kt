package no.fdk.fdk_harvester.service

import io.mockk.*
import no.fdk.fdk_harvester.harvester.ConceptHarvester
import no.fdk.fdk_harvester.harvester.DataServiceHarvester
import no.fdk.fdk_harvester.harvester.DatasetHarvester
import no.fdk.fdk_harvester.harvester.EventHarvester
import no.fdk.fdk_harvester.harvester.InformationModelHarvester
import no.fdk.fdk_harvester.harvester.ServiceHarvester
import no.fdk.fdk_harvester.model.FdkIdAndUri
import no.fdk.fdk_harvester.model.HarvestReport
import no.fdk.fdk_harvester.model.HarvestSourceEntity
import no.fdk.fdk_harvester.model.ResourceEntity
import no.fdk.fdk_harvester.model.ResourceType
import no.fdk.fdk_harvester.repository.HarvestSourceRepository
import no.fdk.fdk_harvester.repository.ResourceRepository
import no.fdk.harvest.DataType
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import java.time.Instant
import java.util.Calendar

@Tag("unit")
class HarvestServiceTest {

    private val conceptHarvester: ConceptHarvester? = mockk()
    private val datasetHarvester: DatasetHarvester? = mockk()
    private val dataServiceHarvester: DataServiceHarvester? = mockk()
    private val informationModelHarvester: InformationModelHarvester? = mockk()
    private val serviceHarvester: ServiceHarvester? = mockk()
    private val eventHarvester: EventHarvester? = mockk()
    private val resourceRepository: ResourceRepository = mockk()
    private val harvestSourceRepository: HarvestSourceRepository = mockk()

    private val harvestService = HarvestService(
        conceptHarvester = conceptHarvester,
        datasetHarvester = datasetHarvester,
        dataServiceHarvester = dataServiceHarvester,
        informationModelHarvester = informationModelHarvester,
        serviceHarvester = serviceHarvester,
        eventHarvester = eventHarvester,
        resourceRepository = resourceRepository,
        harvestSourceRepository = harvestSourceRepository
    )

    private fun initializedSource(uri: String = "http://example.org/source") =
        HarvestSourceEntity(id = 1L, uri = uri, checksum = "c", issued = Instant.now(), initialized = true)

    @BeforeEach
    fun stubHarvestSourceAsInitialized() {
        every { harvestSourceRepository.findByUri(any()) } returns initializedSource()
    }

    @Test
    fun `HarvestService has non-null logger so logging never throws NPE`() {
        val loggerMethod = HarvestService::class.java.getDeclaredMethod("logger")
        loggerMethod.isAccessible = true
        assertNotNull(loggerMethod.invoke(harvestService))
    }

    @Test
    fun `executeHarvest concept delegates to conceptHarvester`() {
        val report = createReport("concept")
        every {
            conceptHarvester?.harvestConceptCollection(any(), any(), any(), any())
        } returns report

        val result = harvestService.executeHarvest(
            dataSourceId = "ds-1",
            dataSourceUrl = "http://example.org/source",
            dataType = DataType.concept,
            acceptHeader = "text/turtle",
            runId = "run-1",
            forced = false
        )

        assertEquals(report, result)
        verify(exactly = 1) { conceptHarvester?.harvestConceptCollection(any(), any(), any(), any()) }
    }

    @Test
    fun `executeHarvest dataset delegates to datasetHarvester`() {
        val report = createReport("dataset")
        every {
            datasetHarvester?.harvestDatasetCatalog(any(), any(), any(), any())
        } returns report

        val result = harvestService.executeHarvest(
            dataSourceId = "ds-1",
            dataSourceUrl = "http://example.org/source",
            dataType = DataType.dataset,
            acceptHeader = "text/turtle",
            runId = "run-1",
            forced = false
        )

        assertEquals(report, result)
        verify(exactly = 1) { datasetHarvester?.harvestDatasetCatalog(any(), any(), any(), any()) }
    }

    @Test
    fun `executeHarvest dataservice delegates to dataServiceHarvester`() {
        val report = createReport("dataservice")
        every {
            dataServiceHarvester?.harvestDataServiceCatalog(any(), any(), any(), any())
        } returns report

        val result = harvestService.executeHarvest(
            dataSourceId = "ds-1",
            dataSourceUrl = "http://example.org/source",
            dataType = DataType.dataservice,
            acceptHeader = "text/turtle",
            runId = "run-1",
            forced = false
        )

        assertEquals(report, result)
        verify(exactly = 1) { dataServiceHarvester?.harvestDataServiceCatalog(any(), any(), any(), any()) }
    }

    @Test
    fun `executeHarvest informationmodel delegates to informationModelHarvester`() {
        val report = createReport("informationmodel")
        every {
            informationModelHarvester?.harvestInformationModelCatalog(any(), any(), any(), any())
        } returns report

        val result = harvestService.executeHarvest(
            dataSourceId = "ds-1",
            dataSourceUrl = "http://example.org/source",
            dataType = DataType.informationmodel,
            acceptHeader = "text/turtle",
            runId = "run-1",
            forced = false
        )

        assertEquals(report, result)
        verify(exactly = 1) { informationModelHarvester?.harvestInformationModelCatalog(any(), any(), any(), any()) }
    }

    @Test
    fun `executeHarvest service delegates to serviceHarvester`() {
        val report = createReport("service")
        every {
            serviceHarvester?.harvestServices(any(), any(), any(), any())
        } returns report

        val result = harvestService.executeHarvest(
            dataSourceId = "ds-1",
            dataSourceUrl = "http://example.org/source",
            dataType = DataType.service,
            acceptHeader = "text/turtle",
            runId = "run-1",
            forced = false
        )

        assertEquals(report, result)
        verify(exactly = 1) { serviceHarvester?.harvestServices(any(), any(), any(), any()) }
    }

    @Test
    fun `executeHarvest publicService delegates to serviceHarvester`() {
        val report = createReport("publicService")
        every {
            serviceHarvester?.harvestServices(any(), any(), any(), any())
        } returns report

        val result = harvestService.executeHarvest(
            dataSourceId = "ds-1",
            dataSourceUrl = "http://example.org/source",
            dataType = DataType.publicService,
            acceptHeader = "text/turtle",
            runId = "run-1",
            forced = false
        )

        assertEquals(report, result)
        verify(exactly = 1) { serviceHarvester?.harvestServices(any(), any(), any(), any()) }
    }

    @Test
    fun `executeHarvest event delegates to eventHarvester`() {
        val report = createReport("event")
        every {
            eventHarvester?.harvestEvents(any(), any(), any(), any())
        } returns report

        val result = harvestService.executeHarvest(
            dataSourceId = "ds-1",
            dataSourceUrl = "http://example.org/source",
            dataType = DataType.event,
            acceptHeader = "text/turtle",
            runId = "run-1",
            forced = false
        )

        assertEquals(report, result)
        verify(exactly = 1) { eventHarvester?.harvestEvents(any(), any(), any(), any()) }
    }

    @Test
    fun `executeHarvest when source not initialized runs with forced true and marks initialized after success`() {
        every { harvestSourceRepository.findByUri("http://example.org/source") } returns null andThen
            HarvestSourceEntity(id = 1L, uri = "http://example.org/source", checksum = "c", issued = Instant.now(), initialized = false)
        every { conceptHarvester?.harvestConceptCollection(any(), any(), any(), any()) } returns createReport("concept")
        every { harvestSourceRepository.save(any<HarvestSourceEntity>()) } answers { firstArg() }

        val result = harvestService.executeHarvest(
            dataSourceId = "ds-1",
            dataSourceUrl = "http://example.org/source",
            dataType = DataType.concept,
            acceptHeader = "text/turtle",
            runId = "run-1",
            forced = false
        )

        assertNotNull(result)
        verify(exactly = 1) { conceptHarvester?.harvestConceptCollection(any(), any(), eq(true), any()) }
        val saveSlot = slot<HarvestSourceEntity>()
        verify(exactly = 1) { harvestSourceRepository.save(capture(saveSlot)) }
        assertTrue(saveSlot.captured.initialized)
    }

    @Test
    fun `executeHarvest when harvester throws rethrows exception`() {
        every {
            conceptHarvester?.harvestConceptCollection(any(), any(), any(), any())
        } throws RuntimeException("harvest failed")

        assertThrows(RuntimeException::class.java) {
            harvestService.executeHarvest(
                dataSourceId = "ds-1",
                dataSourceUrl = "http://example.org/source",
                dataType = DataType.concept,
                acceptHeader = "text/turtle",
                runId = "run-1",
                forced = false
            )
        }
    }

    @Test
    fun `markResourcesAsDeleted when source not found throws`() {
        every { harvestSourceRepository.findByUri("http://example.org/source") } returns null

        assertThrows(IllegalArgumentException::class.java) {
            harvestService.markResourcesAsDeleted(
                sourceUrl = "http://example.org/source",
                dataType = DataType.dataset,
                dataSourceId = "ds-1",
                runId = "run-1"
            )
        }
    }

    @Test
    fun `markResourcesAsDeleted with no resources returns report with empty removed`() {
        val source = HarvestSourceEntity(id = 1L, uri = "http://example.org/source", checksum = "c", issued = Instant.now())
        every { harvestSourceRepository.findByUri("http://example.org/source") } returns source
        every { resourceRepository.findAllByHarvestSourceId(1L) } returns emptyList()

        val result: HarvestReport = harvestService.markResourcesAsDeleted(
            sourceUrl = "http://example.org/source",
            dataType = DataType.dataset,
            dataSourceId = "ds-1",
            runId = "run-1"
        )

        assertEquals("run-1", result.runId)
        assertEquals("ds-1", result.dataSourceId)
        assertEquals("http://example.org/source", result.dataSourceUrl)
        assertEquals("dataset", result.dataType)
        assertEquals(emptyList<FdkIdAndUri>(), result.removedResources)
    }

    @Test
    fun `markResourcesAsDeleted with resources marks removed and returns report`() {
        val source = HarvestSourceEntity(id = 1L, uri = "http://example.org/source", checksum = "c", issued = Instant.now())
        val resource = ResourceEntity(
            uri = "http://example.org/dataset1",
            type = ResourceType.DATASET,
            fdkId = "fdk-1",
            removed = false,
            issued = Instant.now(),
            modified = Instant.now(),
            checksum = "x",
            harvestSource = source
        )
        every { harvestSourceRepository.findByUri("http://example.org/source") } returns source
        every { resourceRepository.findAllByHarvestSourceId(1L) } returns listOf(resource)
        every { resourceRepository.save(any<ResourceEntity>()) } answers { firstArg() }

        val result = harvestService.markResourcesAsDeleted(
            sourceUrl = "http://example.org/source",
            dataType = DataType.dataset,
            dataSourceId = "ds-1",
            runId = "run-1"
        )

        assertEquals(1, result.removedResources.size)
        assertEquals("fdk-1", result.removedResources[0].fdkId)
        assertEquals("http://example.org/dataset1", result.removedResources[0].uri)
        verify(exactly = 1) { resourceRepository.save(any<ResourceEntity>()) }
    }

    @Test
    fun `markResourcesAsDeleted filters by dataType`() {
        val source = HarvestSourceEntity(id = 1L, uri = "http://example.org/source", checksum = "c", issued = Instant.now())
        val datasetResource = ResourceEntity(
            uri = "http://example.org/dataset1",
            type = ResourceType.DATASET,
            fdkId = "fdk-ds",
            removed = false,
            issued = Instant.now(),
            modified = Instant.now(),
            checksum = "x",
            harvestSource = source
        )
        val conceptResource = ResourceEntity(
            uri = "http://example.org/concept1",
            type = ResourceType.CONCEPT,
            fdkId = "fdk-c",
            removed = false,
            issued = Instant.now(),
            modified = Instant.now(),
            checksum = "y",
            harvestSource = source
        )
        every { harvestSourceRepository.findByUri("http://example.org/source") } returns source
        every { resourceRepository.findAllByHarvestSourceId(1L) } returns listOf(datasetResource, conceptResource)
        every { resourceRepository.save(any<ResourceEntity>()) } answers { firstArg() }

        val result = harvestService.markResourcesAsDeleted(
            sourceUrl = "http://example.org/source",
            dataType = DataType.dataset,
            dataSourceId = "ds-1",
            runId = "run-1"
        )

        // Only DATASET should be updated
        assertEquals(1, result.removedResources.size)
        assertEquals("fdk-ds", result.removedResources[0].fdkId)
        verify(exactly = 1) { resourceRepository.save(any<ResourceEntity>()) }
    }

    @Test
    fun `markResourcesAsDeleted skips already removed resources`() {
        val source = HarvestSourceEntity(id = 1L, uri = "http://example.org/source", checksum = "c", issued = Instant.now())
        val removedResource = ResourceEntity(
            uri = "http://example.org/dataset1",
            type = ResourceType.DATASET,
            fdkId = "fdk-1",
            removed = true,
            issued = Instant.now(),
            modified = Instant.now(),
            checksum = "x",
            harvestSource = source
        )
        every { harvestSourceRepository.findByUri("http://example.org/source") } returns source
        every { resourceRepository.findAllByHarvestSourceId(1L) } returns listOf(removedResource)

        val result = harvestService.markResourcesAsDeleted(
            sourceUrl = "http://example.org/source",
            dataType = DataType.dataset,
            dataSourceId = "ds-1",
            runId = "run-1"
        )

        assertEquals(0, result.removedResources.size)
        verify(exactly = 0) { resourceRepository.save(any<ResourceEntity>()) }
    }

    private fun createReport(dataType: String): HarvestReport = HarvestReport(
        runId = "run-1",
        dataSourceId = "ds-1",
        dataSourceUrl = "http://example.org/source",
        dataType = dataType,
        harvestError = false,
        startTime = "2024-01-01T00:00:00",
        endTime = "2024-01-01T00:00:01",
        changedCatalogs = emptyList(),
        changedResources = emptyList(),
        removedResources = emptyList()
    )
}
