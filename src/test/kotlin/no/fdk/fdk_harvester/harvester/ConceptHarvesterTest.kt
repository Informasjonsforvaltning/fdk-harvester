package no.fdk.fdk_harvester.harvester

import no.fdk.fdk_harvester.adapter.DefaultOrganizationsAdapter
import no.fdk.fdk_harvester.config.ApplicationProperties
import no.fdk.fdk_harvester.kafka.ResourceEventProducer
import no.fdk.fdk_harvester.model.*
import no.fdk.fdk_harvester.repository.HarvestSourceRepository
import no.fdk.fdk_harvester.repository.ResourceRepository
import io.mockk.*
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import java.util.*

@Tag("unit")
class ConceptHarvesterTest {

    private val orgAdapter: DefaultOrganizationsAdapter = mockk()
    private val resourceRepository: ResourceRepository = mockk()
    private val resourceEventProducer: ResourceEventProducer = mockk(relaxed = true)
    private val harvestSourceRepository: HarvestSourceRepository = mockk()

    private lateinit var conceptHarvester: ConceptHarvester

    @BeforeEach
    fun setUp() {
        clearAllMocks()
        val appProps = ApplicationProperties(
            conceptUri = "https://concepts.fellesdatakatalog.digdir.no/concepts"
        )
        conceptHarvester = ConceptHarvester(
            orgAdapter,
            resourceRepository,
            resourceEventProducer,
            appProps,
            harvestSourceRepository
        )
    }

    @Test
    fun `test harvestConceptCollection with invalid source returns null`() {
        val source = HarvestDataSource(
            id = null,
            url = "http://example.org/source",
            acceptHeaderValue = "text/turtle"
        )
        val harvestDate = Calendar.getInstance()

        val report = conceptHarvester.harvestConceptCollection(source, harvestDate, false, "run-123")

        assertNull(report)
    }

    @Test
    fun `test harvestConceptCollection with missing accept header returns null`() {
        val source = HarvestDataSource(
            id = "source-1",
            url = "http://example.org/source",
            acceptHeaderValue = null
        )
        val harvestDate = Calendar.getInstance()

        val report = conceptHarvester.harvestConceptCollection(source, harvestDate, false, "run-123")

        assertNull(report)
    }

    @Test
    fun `test harvestConceptCollection with invalid accept header returns error report`() {
        val source = HarvestDataSource(
            id = "source-1",
            url = "http://example.org/source",
            acceptHeaderValue = "unknown/type"
        )
        val harvestDate = Calendar.getInstance()

        val report = conceptHarvester.harvestConceptCollection(source, harvestDate, false, "run-123")

        assertNotNull(report)
        assertTrue(report?.harvestError ?: false)
        assertNotNull(report?.errorMessage)
    }
}

