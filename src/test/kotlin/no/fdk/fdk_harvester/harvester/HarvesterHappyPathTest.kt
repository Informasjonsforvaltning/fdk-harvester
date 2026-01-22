package no.fdk.fdk_harvester.harvester

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock.aResponse
import com.github.tomakehurst.wiremock.client.WireMock.get
import com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import io.mockk.*
import no.fdk.fdk_harvester.adapter.DefaultOrganizationsAdapter
import no.fdk.fdk_harvester.config.ApplicationProperties
import no.fdk.fdk_harvester.kafka.ResourceEventProducer
import no.fdk.harvest.DataType
import no.fdk.fdk_harvester.model.HarvestDataSource
import no.fdk.fdk_harvester.model.HarvestSourceEntity
import no.fdk.fdk_harvester.model.Organization
import no.fdk.fdk_harvester.model.PrefLabel
import no.fdk.fdk_harvester.model.ResourceEntity
import no.fdk.fdk_harvester.model.ResourceType
import no.fdk.fdk_harvester.repository.HarvestSourceRepository
import no.fdk.fdk_harvester.repository.ResourceRepository
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import java.time.Instant
import java.util.Calendar
import java.util.Optional

@Tag("unit")
class HarvesterHappyPathTest {

    @Test
    fun `dataset harvester - harvestDatasetCatalog happy path`() {
        val ttl = """
            @prefix dcat: <http://www.w3.org/ns/dcat#> .
            @prefix dct: <http://purl.org/dc/terms/> .
            <http://example.org/catalog> a dcat:Catalog ;
              dcat:dataset <http://example.org/dataset1> .
            <http://example.org/dataset1> a dcat:Dataset ;
              dct:title "Dataset 1" .
        """.trimIndent()

        val server = wireMock(ttl)
        try {
            val resourceRepository = mockResourceRepository()
            val harvestSourceRepository = mockHarvestSourceRepository()
            val resourceGraphsSlot = slot<Map<String, String>>()
            val producer = mockk<ResourceEventProducer>(relaxed = true)

            val appProps = ApplicationProperties(
                datasetUri = "https://datasets.fellesdatakatalog.digdir.no/datasets"
            )
            val harvester = DatasetHarvester(appProps, resourceRepository, harvestSourceRepository, producer)
            val report = harvester.harvestDatasetCatalog(
                source = HarvestDataSource(
                    id = "source-1",
                    url = "http://localhost:${server.port()}/rdf",
                    acceptHeaderValue = "text/turtle"
                ),
                harvestDate = Calendar.getInstance(),
                forceUpdate = false,
                runId = "run-1"
            )

            assertNotNull(report)
            assertFalse(report!!.harvestError)
            assertTrue(report.changedCatalogs.isNotEmpty(), "report should contain changed catalogs")
            assertTrue(report.changedResources.isNotEmpty(), "report should contain changed resources (datasets)")
            verify(atLeast = 1) { resourceRepository.save(any<ResourceEntity>()) }
            verify(atLeast = 1) {
                producer.publishHarvestedEvents(
                    dataType = DataType.dataset,
                    resources = any(),
                    resourceGraphs = capture(resourceGraphsSlot),
                    runId = any()
                )
            }
            val graphs = resourceGraphsSlot.captured
            assertTrue(graphs.isNotEmpty(), "produced graphs should not be empty")
            val graphWithRecord = graphs.values.first()
            assertTrue(
                graphWithRecord.contains("CatalogRecord") || graphWithRecord.contains("dcat:CatalogRecord"),
                "produced graph should contain catalog record (dcat:CatalogRecord)"
            )
            assertTrue(
                graphWithRecord.contains("isPartOf") || graphWithRecord.contains("dct:isPartOf"),
                "produced graph should contain dct:isPartOf for record"
            )
        } finally {
            server.stop()
        }
    }

    @Test
    fun `data service harvester - harvestDataServiceCatalog happy path`() {
        val ttl = """
            @prefix dcat: <http://www.w3.org/ns/dcat#> .
            @prefix dct: <http://purl.org/dc/terms/> .
            <http://example.org/catalog> a dcat:Catalog ;
              dcat:service <http://example.org/ds1> .
            <http://example.org/ds1> a dcat:DataService ;
              dct:title "Data service 1" .
        """.trimIndent()

        val server = wireMock(ttl)
        try {
            val resourceRepository = mockResourceRepository()
            val harvestSourceRepository = mockHarvestSourceRepository()
            val producer = mockk<ResourceEventProducer>(relaxed = true)

            val appProps = ApplicationProperties(
                dataserviceUri = "https://dataservices.fellesdatakatalog.digdir.no/dataservices"
            )
            val harvester = DataServiceHarvester(appProps, resourceRepository, producer, harvestSourceRepository)
            val report = harvester.harvestDataServiceCatalog(
                source = HarvestDataSource(
                    id = "source-1",
                    url = "http://localhost:${server.port()}/rdf",
                    acceptHeaderValue = "text/turtle"
                ),
                harvestDate = Calendar.getInstance(),
                forceUpdate = false,
                runId = "run-1"
            )

            assertNotNull(report)
            assertFalse(report!!.harvestError)
            verify(atLeast = 1) { resourceRepository.save(any<ResourceEntity>()) }
        } finally {
            server.stop()
        }
    }

    @Test
    fun `information model harvester - harvestInformationModelCatalog happy path`() {
        val ttl = """
            @prefix dcat: <http://www.w3.org/ns/dcat#> .
            @prefix modell: <https://data.norge.no/vocabulary/modelldcatno#> .
            <http://example.org/catalog> a dcat:Catalog ;
              modell:model <http://example.org/im1> .
            <http://example.org/im1> a modell:InformationModel .
        """.trimIndent()

        val server = wireMock(ttl)
        try {
            val resourceRepository = mockResourceRepository()
            val harvestSourceRepository = mockHarvestSourceRepository()
            val producer = mockk<ResourceEventProducer>(relaxed = true)

            val appProps = ApplicationProperties(
                informationmodelUri = "https://informationmodels.fellesdatakatalog.digdir.no/informationmodels"
            )
            val harvester = InformationModelHarvester(appProps, resourceRepository, harvestSourceRepository, producer)
            val report = harvester.harvestInformationModelCatalog(
                source = HarvestDataSource(
                    id = "source-1",
                    url = "http://localhost:${server.port()}/rdf",
                    acceptHeaderValue = "text/turtle"
                ),
                harvestDate = Calendar.getInstance(),
                forceUpdate = false,
                runId = "run-1"
            )

            assertNotNull(report)
            assertFalse(report!!.harvestError)
            verify(atLeast = 1) { resourceRepository.save(any<ResourceEntity>()) }
        } finally {
            server.stop()
        }
    }

    @Test
    fun `information model harvester - includes code lists and code elements`() {
        val ttl = """
            @prefix dcat: <http://www.w3.org/ns/dcat#> .
            @prefix modell: <https://data.norge.no/vocabulary/modelldcatno#> .
            @prefix skos: <http://www.w3.org/2004/02/skos/core#> .
            <http://example.org/catalog> a dcat:Catalog ;
              modell:model <http://example.org/im1> .
            <http://example.org/im1> a modell:InformationModel ;
              modell:containsModelElement _:cl1 .
            _:cl1 a modell:CodeList .
            _:ce1 a modell:CodeElement ;
              skos:inScheme _:cl1 .
        """.trimIndent()

        val server = wireMock(ttl)
        try {
            val resourceRepository = mockResourceRepository()
            val harvestSourceRepository = mockHarvestSourceRepository()
            val producer = mockk<ResourceEventProducer>(relaxed = true)

            val appProps = ApplicationProperties(
                informationmodelUri = "https://informationmodels.fellesdatakatalog.digdir.no/informationmodels"
            )
            val harvester = InformationModelHarvester(appProps, resourceRepository, harvestSourceRepository, producer)
            val report = harvester.harvestInformationModelCatalog(
                source = HarvestDataSource(
                    id = "source-1",
                    url = "http://localhost:${server.port()}/rdf",
                    acceptHeaderValue = "text/turtle"
                ),
                harvestDate = Calendar.getInstance(),
                forceUpdate = false,
                runId = "run-1"
            )

            assertNotNull(report)
            assertFalse(report!!.harvestError)
            verify(atLeast = 1) { resourceRepository.save(any<ResourceEntity>()) }
        } finally {
            server.stop()
        }
    }

    @Test
    fun `concept harvester - harvestConceptCollection happy path`() {
        val ttl = """
            @prefix skos: <http://www.w3.org/2004/02/skos/core#> .
            <http://example.org/collection> a skos:Collection ;
              skos:member <http://example.org/concept1> .
            <http://example.org/concept1> a skos:Concept .
        """.trimIndent()

        val server = wireMock(ttl)
        try {
            val orgAdapter = mockk<DefaultOrganizationsAdapter>(relaxed = true)
            val resourceRepository = mockResourceRepository()
            val harvestSourceRepository = mockHarvestSourceRepository()
            val producer = mockk<ResourceEventProducer>(relaxed = true)

            val appProps = ApplicationProperties(
                conceptUri = "https://concepts.fellesdatakatalog.digdir.no/concepts"
            )
            val harvester = ConceptHarvester(orgAdapter, resourceRepository, producer, appProps, harvestSourceRepository)
            val report = harvester.harvestConceptCollection(
                source = HarvestDataSource(
                    id = "source-1",
                    url = "http://localhost:${server.port()}/rdf",
                    acceptHeaderValue = "text/turtle",
                    publisherId = null
                ),
                harvestDate = Calendar.getInstance(),
                forceUpdate = false,
                runId = "run-1"
            )

            assertNotNull(report)
            assertFalse(report!!.harvestError)
            verify(atLeast = 1) { resourceRepository.save(any<ResourceEntity>()) }
        } finally {
            server.stop()
        }
    }

    @Test
    fun `service harvester - harvestServices happy path`() {
        val ttl = """
            @prefix dcat: <http://www.w3.org/ns/dcat#> .
            @prefix dcatno: <https://data.norge.no/vocabulary/dcatno#> .
            @prefix cpsvno: <https://data.norge.no/vocabulary/cpsvno#> .
            <http://example.org/catalog> a dcat:Catalog ;
              dcatno:containsService <http://example.org/service1> .
            <http://example.org/service1> a cpsvno:Service .
        """.trimIndent()

        val server = wireMock(ttl)
        try {
            val orgAdapter = mockk<DefaultOrganizationsAdapter>(relaxed = true)
            val resourceRepository = mockResourceRepository()
            val harvestSourceRepository = mockHarvestSourceRepository()
            val producer = mockk<ResourceEventProducer>(relaxed = true)

            val appProps = ApplicationProperties(
                serviceUri = "https://services.fellesdatakatalog.digdir.no/services"
            )
            val harvester = ServiceHarvester(appProps, orgAdapter, resourceRepository, harvestSourceRepository, producer)
            val report = harvester.harvestServices(
                source = HarvestDataSource(
                    id = "source-1",
                    url = "http://localhost:${server.port()}/rdf",
                    acceptHeaderValue = "text/turtle",
                    publisherId = null
                ),
                harvestDate = Calendar.getInstance(),
                forceUpdate = false,
                runId = "run-1"
            )

            assertNotNull(report)
            assertFalse(report!!.harvestError)
            verify(atLeast = 1) { resourceRepository.save(any<ResourceEntity>()) }
        } finally {
            server.stop()
        }
    }

    @Test
    fun `service harvester - generated catalog path with free service and publisher`() {
        val ttl = """
            @prefix dcat: <http://www.w3.org/ns/dcat#> .
            @prefix dcatno: <https://data.norge.no/vocabulary/dcatno#> .
            @prefix cpsvno: <https://data.norge.no/vocabulary/cpsvno#> .
            @prefix dct: <http://purl.org/dc/terms/> .
            <http://example.org/catalog> a dcat:Catalog ;
              dcatno:containsService <http://example.org/service1> .
            <http://example.org/service1> a cpsvno:Service ; dct:title "S1" .
            <http://example.org/serviceFree> a cpsvno:Service ; dct:title "S2" .
        """.trimIndent()

        val server = wireMock(ttl)
        try {
            val orgAdapter = mockk<DefaultOrganizationsAdapter>()
            every { orgAdapter.getOrganization("123") } returns Organization(
                organizationId = "123",
                uri = "http://example.org/org",
                name = "Org",
                prefLabel = PrefLabel(nb = "NB", nn = "NN", en = "EN")
            )
            val resourceRepository = mockResourceRepository()
            val harvestSourceRepository = mockHarvestSourceRepository()
            val producer = mockk<ResourceEventProducer>(relaxed = true)

            val appProps = ApplicationProperties(
                serviceUri = "https://services.fellesdatakatalog.digdir.no/services"
            )
            val harvester = ServiceHarvester(appProps, orgAdapter, resourceRepository, harvestSourceRepository, producer)
            val report = harvester.harvestServices(
                source = HarvestDataSource(
                    id = "source-1",
                    url = "http://localhost:${server.port()}/rdf",
                    acceptHeaderValue = "text/turtle",
                    publisherId = "123"
                ),
                harvestDate = Calendar.getInstance(),
                forceUpdate = false,
                runId = "run-1"
            )

            assertNotNull(report)
            assertFalse(report!!.harvestError)
            verify(atLeast = 1) { orgAdapter.getOrganization("123") }
        } finally {
            server.stop()
        }
    }

    @Test
    fun `event harvester - harvestEvents happy path`() {
        val ttl = """
            @prefix dcat: <http://www.w3.org/ns/dcat#> .
            @prefix dcatno: <https://data.norge.no/vocabulary/dcatno#> .
            @prefix cv: <http://data.europa.eu/m8g/> .
            <http://example.org/catalog> a dcat:Catalog ;
              dcatno:containsEvent <http://example.org/event1> .
            <http://example.org/event1> a cv:Event .
        """.trimIndent()

        val server = wireMock(ttl)
        try {
            val orgAdapter = mockk<DefaultOrganizationsAdapter>(relaxed = true)
            val resourceRepository = mockResourceRepository()
            val harvestSourceRepository = mockHarvestSourceRepository()
            val producer = mockk<ResourceEventProducer>(relaxed = true)

            val appProps = ApplicationProperties(
                eventUri = "https://events.fellesdatakatalog.digdir.no/events"
            )
            val harvester = EventHarvester(appProps, orgAdapter, resourceRepository, producer, harvestSourceRepository)
            val report = harvester.harvestEvents(
                source = HarvestDataSource(
                    id = "source-1",
                    url = "http://localhost:${server.port()}/rdf",
                    acceptHeaderValue = "text/turtle",
                    publisherId = null
                ),
                harvestDate = Calendar.getInstance(),
                forceUpdate = false,
                runId = "run-1"
            )

            assertNotNull(report)
            assertFalse(report!!.harvestError)
            verify(atLeast = 1) { resourceRepository.save(any<ResourceEntity>()) }
        } finally {
            server.stop()
        }
    }

    private fun wireMock(turtle: String): WireMockServer {
        val server = WireMockServer(wireMockConfig().dynamicPort())
        server.start()
        server.stubFor(
            get(urlEqualTo("/rdf"))
                .willReturn(
                    aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "text/turtle")
                        .withBody(turtle)
                )
        )
        return server
    }

    private fun mockHarvestSourceRepository(): HarvestSourceRepository {
        val repo = mockk<HarvestSourceRepository>()
        every { repo.findByUri(any()) } returns null
        every { repo.save(any()) } answers { firstArg<HarvestSourceEntity>().copy(id = 1L) }
        return repo
    }

    private fun mockResourceRepository(): ResourceRepository {
        val repo = mockk<ResourceRepository>()
        every { repo.findById(any<String>()) } returns Optional.empty()
        every { repo.findAllByType(any()) } returns emptyList()
        every { repo.findAllByTypeAndRemoved(any(), any()) } returns emptyList()
        every { repo.findByFdkId(any()) } returns null
        every { repo.findAllByFdkId(any()) } returns emptyList()
        every { repo.save(any<ResourceEntity>()) } answers { firstArg() }
        every { repo.saveAll(any<Iterable<ResourceEntity>>()) } answers { firstArg<Iterable<ResourceEntity>>().toList() }
        return repo
    }
}


