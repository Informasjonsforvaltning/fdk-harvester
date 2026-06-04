package no.fdk.harvester.harvester

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock.aResponse
import com.github.tomakehurst.wiremock.client.WireMock.get
import com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import no.fdk.harvest.DataType
import no.fdk.harvester.adapter.DefaultOrganizationsAdapter
import no.fdk.harvester.config.ApplicationProperties
import no.fdk.harvester.kafka.ResourceEventProducer
import no.fdk.harvester.model.HarvestDataSource
import no.fdk.harvester.model.HarvestSourceEntity
import no.fdk.harvester.model.Organization
import no.fdk.harvester.model.PrefLabel
import no.fdk.harvester.model.ResourceEntity
import no.fdk.harvester.model.ResourceType
import no.fdk.harvester.repository.HarvestSourceRepository
import no.fdk.harvester.repository.ResourceRepository
import org.junit.jupiter.api.Assertions.assertEquals
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
        val ttl =
            """
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

            val appProps =
                ApplicationProperties(
                    datasetUri = "https://datasets.fellesdatakatalog.digdir.no/datasets",
                )
            val harvester = DatasetHarvester(appProps, mockk(relaxed = true), resourceRepository, harvestSourceRepository, producer)
            val report =
                harvester.harvestDatasetCatalog(
                    source =
                        HarvestDataSource(
                            id = "source-1",
                            url = "http://localhost:${server.port()}/rdf",
                            acceptHeaderValue = "text/turtle",
                        ),
                    harvestDate = Calendar.getInstance(),
                    forceUpdate = false,
                    runId = "run-1",
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
                    runId = any(),
                    catalogGraphs = any(),
                )
            }
            val graphs = resourceGraphsSlot.captured
            assertTrue(graphs.isNotEmpty(), "produced graphs should not be empty")
            val graphWithRecord = graphs.values.first()
            assertTrue(
                graphWithRecord.contains("CatalogRecord") || graphWithRecord.contains("dcat:CatalogRecord"),
                "produced graph should contain catalog record (dcat:CatalogRecord)",
            )
            assertTrue(
                graphWithRecord.contains("isPartOf") || graphWithRecord.contains("dct:isPartOf"),
                "produced graph should contain dct:isPartOf for record",
            )
        } finally {
            server.stop()
        }
    }

    @Test
    fun `dataset harvester - skips conflicting dataset and continues with remaining resources`() {
        val conflictDatasetUri = "http://example.org/dataset-conflict"
        val validDatasetUri = "http://example.org/dataset-ok"
        val ttl =
            """
            @prefix dcat: <http://www.w3.org/ns/dcat#> .
            @prefix dct: <http://purl.org/dc/terms/> .
            <http://example.org/catalog> a dcat:Catalog ;
              dcat:dataset <$conflictDatasetUri> ;
              dcat:dataset <$validDatasetUri> .
            <$conflictDatasetUri> a dcat:Dataset ; dct:title "Conflicting dataset" .
            <$validDatasetUri> a dcat:Dataset ; dct:title "Valid dataset" .
            """.trimIndent()

        val server = wireMock(ttl)
        try {
            val resourceRepository =
                mockResourceRepositoryWithConflicts(
                    mapOf(conflictDatasetUri to ResourceType.DATASET),
                )
            val harvestSourceRepository = mockHarvestSourceRepository()
            val producer = mockk<ResourceEventProducer>(relaxed = true)

            val appProps =
                ApplicationProperties(
                    datasetUri = "https://datasets.fellesdatakatalog.digdir.no/datasets",
                )
            val harvester = DatasetHarvester(appProps, mockk(relaxed = true), resourceRepository, harvestSourceRepository, producer)
            val report =
                harvester.harvestDatasetCatalog(
                    source =
                        HarvestDataSource(
                            id = "source-1",
                            url = "http://localhost:${server.port()}/rdf",
                            acceptHeaderValue = "text/turtle",
                        ),
                    harvestDate = Calendar.getInstance(),
                    forceUpdate = false,
                    runId = "run-1",
                )

            assertNotNull(report)
            assertFalse(report!!.harvestError)
            assertEquals(1, report.changedResources.size, "only non-conflicting dataset should be harvested")
            assertEquals(validDatasetUri, report.changedResources.first().uri)
        } finally {
            server.stop()
        }
    }

    @Test
    fun `data service harvester - harvestDataServiceCatalog happy path`() {
        val ttl =
            """
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

            val appProps =
                ApplicationProperties(
                    dataserviceUri = "https://dataservices.fellesdatakatalog.digdir.no/dataservices",
                )
            val harvester = DataServiceHarvester(appProps, mockk(relaxed = true), resourceRepository, harvestSourceRepository, producer)
            val report =
                harvester.harvestDataServiceCatalog(
                    source =
                        HarvestDataSource(
                            id = "source-1",
                            url = "http://localhost:${server.port()}/rdf",
                            acceptHeaderValue = "text/turtle",
                        ),
                    harvestDate = Calendar.getInstance(),
                    forceUpdate = false,
                    runId = "run-1",
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
        val ttl =
            """
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

            val appProps =
                ApplicationProperties(
                    informationmodelUri = "https://informationmodels.fellesdatakatalog.digdir.no/informationmodels",
                )
            val harvester =
                InformationModelHarvester(appProps, mockk(relaxed = true), resourceRepository, harvestSourceRepository, producer)
            val report =
                harvester.harvestInformationModelCatalog(
                    source =
                        HarvestDataSource(
                            id = "source-1",
                            url = "http://localhost:${server.port()}/rdf",
                            acceptHeaderValue = "text/turtle",
                        ),
                    harvestDate = Calendar.getInstance(),
                    forceUpdate = false,
                    runId = "run-1",
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
        val ttl =
            """
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
            val resourceGraphsSlot = slot<Map<String, String>>()
            val producer = mockk<ResourceEventProducer>(relaxed = true)

            val appProps =
                ApplicationProperties(
                    informationmodelUri = "https://informationmodels.fellesdatakatalog.digdir.no/informationmodels",
                )
            val harvester =
                InformationModelHarvester(appProps, mockk(relaxed = true), resourceRepository, harvestSourceRepository, producer)
            val report =
                harvester.harvestInformationModelCatalog(
                    source =
                        HarvestDataSource(
                            id = "source-1",
                            url = "http://localhost:${server.port()}/rdf",
                            acceptHeaderValue = "text/turtle",
                        ),
                    harvestDate = Calendar.getInstance(),
                    forceUpdate = false,
                    runId = "run-1",
                )

            assertNotNull(report)
            assertFalse(report!!.harvestError)
            verify(atLeast = 1) { resourceRepository.save(any<ResourceEntity>()) }
            verify(atLeast = 1) {
                producer.publishHarvestedEvents(
                    dataType = DataType.informationmodel,
                    resources = any(),
                    resourceGraphs = capture(resourceGraphsSlot),
                    runId = any(),
                    catalogGraphs = any(),
                )
            }

            val infoModelGraph = resourceGraphsSlot.captured.values.first()
            assertTrue(
                infoModelGraph.contains("CodeList"),
                "produced graph should inline the referenced (blank node) code list",
            )
            assertTrue(
                infoModelGraph.contains("CodeElement") && infoModelGraph.contains("inScheme"),
                "produced graph should inline the code element associated with the code list via skos:inScheme",
            )
        } finally {
            server.stop()
        }
    }

    @Test
    fun `concept harvester - harvestConceptCollection happy path`() {
        val ttl =
            """
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

            val appProps =
                ApplicationProperties(
                    conceptUri = "https://concepts.fellesdatakatalog.digdir.no/concepts",
                )
            val harvester = ConceptHarvester(appProps, orgAdapter, resourceRepository, harvestSourceRepository, producer)
            val report =
                harvester.harvestConceptCollection(
                    source =
                        HarvestDataSource(
                            id = "source-1",
                            url = "http://localhost:${server.port()}/rdf",
                            acceptHeaderValue = "text/turtle",
                            publisherId = null,
                        ),
                    harvestDate = Calendar.getInstance(),
                    forceUpdate = false,
                    runId = "run-1",
                )

            assertNotNull(report)
            assertFalse(report!!.harvestError)
            verify(atLeast = 1) { resourceRepository.save(any<ResourceEntity>()) }
        } finally {
            server.stop()
        }
    }

    @Test
    fun `concept harvester - skips conflicting concept and continues with remaining concepts`() {
        val conflictConceptUri = "http://example.org/concept-conflict"
        val validConceptUri = "http://example.org/concept-ok"
        val ttl =
            """
            @prefix skos: <http://www.w3.org/2004/02/skos/core#> .
            <http://example.org/collection> a skos:Collection ;
              skos:member <$conflictConceptUri> ;
              skos:member <$validConceptUri> .
            <$conflictConceptUri> a skos:Concept .
            <$validConceptUri> a skos:Concept .
            """.trimIndent()

        val server = wireMock(ttl)
        try {
            val orgAdapter = mockk<DefaultOrganizationsAdapter>(relaxed = true)
            val resourceRepository =
                mockResourceRepositoryWithConflicts(
                    mapOf(conflictConceptUri to ResourceType.CONCEPT),
                )
            val harvestSourceRepository = mockHarvestSourceRepository()
            val producer = mockk<ResourceEventProducer>(relaxed = true)

            val appProps =
                ApplicationProperties(
                    conceptUri = "https://concepts.fellesdatakatalog.digdir.no/concepts",
                )
            val harvester = ConceptHarvester(appProps, orgAdapter, resourceRepository, harvestSourceRepository, producer)
            val report =
                harvester.harvestConceptCollection(
                    source =
                        HarvestDataSource(
                            id = "source-1",
                            url = "http://localhost:${server.port()}/rdf",
                            acceptHeaderValue = "text/turtle",
                            publisherId = null,
                        ),
                    harvestDate = Calendar.getInstance(),
                    forceUpdate = false,
                    runId = "run-1",
                )

            assertNotNull(report)
            assertFalse(report!!.harvestError)
            assertEquals(1, report.changedResources.size, "only non-conflicting concept should be harvested")
            assertEquals(validConceptUri, report.changedResources.first().uri)
        } finally {
            server.stop()
        }
    }

    @Test
    fun `service harvester - harvestServices happy path`() {
        val ttl =
            """
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

            val appProps =
                ApplicationProperties(
                    serviceUri = "https://services.fellesdatakatalog.digdir.no/services",
                )
            val harvester = ServiceHarvester(appProps, orgAdapter, resourceRepository, harvestSourceRepository, producer)
            val report =
                harvester.harvestServices(
                    source =
                        HarvestDataSource(
                            id = "source-1",
                            url = "http://localhost:${server.port()}/rdf",
                            acceptHeaderValue = "text/turtle",
                            publisherId = null,
                        ),
                    harvestDate = Calendar.getInstance(),
                    forceUpdate = false,
                    runId = "run-1",
                )

            assertNotNull(report)
            assertFalse(report!!.harvestError)
            verify(atLeast = 1) { resourceRepository.save(any<ResourceEntity>()) }
        } finally {
            server.stop()
        }
    }

    @Test
    fun `service harvester - harvests public service with output and required evidence`() {
        val serviceUri =
            "https://service-catalog.api.staging.fellesdatakatalog.digdir.no/rdf/catalogs/910244132/" +
                "public-services/9e576bc5-6a8f-44ae-bd04-ce74be200727"
        val ttl =
            """
            PREFIX adms:   <http://www.w3.org/ns/adms#>
            PREFIX cpsv:   <http://purl.org/vocab/cpsv#>
            PREFIX cv:     <http://data.europa.eu/m8g/>
            PREFIX dcat:   <http://www.w3.org/ns/dcat#>
            PREFIX dcatno: <https://data.norge.no/vocabulary/dcatno#>
            PREFIX dct:    <http://purl.org/dc/terms/>
            PREFIX foaf:   <http://xmlns.com/foaf/0.1/>
            PREFIX vcard:  <http://www.w3.org/2006/vcard/ns#>

            <$serviceUri/output/0>
                    a                cv:Output;
                    dct:description  "Produserer-beskrivelse"@nb;
                    dct:title        "Produserer-tittel"@nb .

            <$serviceUri/evidence/1>
                    a                <https://data.norge.no/vocabulary/cpsvno#RequiredEvidence>;
                    dct:description  "beskrivelse"@nb;
                    dct:identifier   <$serviceUri/evidence/1>;
                    dct:language     <http://publications.europa.eu/resource/authority/language/NOB>;
                    dct:title        "dokumentasjon 2"@nb .

            <$serviceUri>
                    a                         cpsv:PublicService;
                    cv:hasCompetentAuthority  <https://data.brreg.no/enhetsregisteret/api/enheter/910244132>;
                    dct:description           "Tilfeldig beskrivelse"@nb;
                    dct:identifier            <$serviceUri>;
                    dct:title                 "Eksempeltjeneste"@nb;
                    cpsv:produces             <$serviceUri/output/0>;
                    <https://data.norge.no/vocabulary/cpsvno#hasRequiredEvidence>
                            <$serviceUri/evidence/0> , <$serviceUri/evidence/1> .

            <$serviceUri/evidence/0>
                    a                <https://data.norge.no/vocabulary/cpsvno#RequiredEvidence>;
                    dct:description  "Beskrivelse av dokumentasjonskrav"@nb;
                    dct:identifier   <$serviceUri/evidence/0>;
                    dct:isPartOf     <https://registrering.staging.fellesdatakatalog.digdir.no/catalogs/974760673/datasets/f94bd890-accd-4177-86d7-bb63d35ebda3>;
                    dct:language     <http://publications.europa.eu/resource/authority/language/NNO> , <http://publications.europa.eu/resource/authority/language/NOB> , <http://publications.europa.eu/resource/authority/language/ENG>;
                    dct:title        "Navn på dokumentasjonskrav"@nb , "Name"@en;
                    foaf:page        <https://dokumentasjon.no> .
            """.trimIndent()

        val server = wireMock(ttl)
        try {
            val orgAdapter = mockk<DefaultOrganizationsAdapter>(relaxed = true)
            val resourceRepository = mockResourceRepository()
            val harvestSourceRepository = mockHarvestSourceRepository()
            val resourceGraphsSlot = slot<Map<String, String>>()
            val producer = mockk<ResourceEventProducer>(relaxed = true)

            val appProps =
                ApplicationProperties(
                    serviceUri = "https://services.fellesdatakatalog.digdir.no/services",
                )
            val harvester = ServiceHarvester(appProps, orgAdapter, resourceRepository, harvestSourceRepository, producer)
            val report =
                harvester.harvestServices(
                    source =
                        HarvestDataSource(
                            id = "source-1",
                            url = "http://localhost:${server.port()}/rdf",
                            acceptHeaderValue = "text/turtle",
                            publisherId = null,
                        ),
                    harvestDate = Calendar.getInstance(),
                    forceUpdate = false,
                    runId = "run-1",
                )

            assertNotNull(report)
            assertFalse(report!!.harvestError)
            assertEquals(1, report.changedResources.size, "the single public service should be harvested")
            assertEquals(serviceUri, report.changedResources.first().uri)
            verify(atLeast = 1) { resourceRepository.save(any<ResourceEntity>()) }
            verify(atLeast = 1) {
                producer.publishHarvestedEvents(
                    dataType = DataType.service,
                    resources = any(),
                    resourceGraphs = capture(resourceGraphsSlot),
                    runId = any(),
                    catalogGraphs = any(),
                )
            }

            val serviceGraph = resourceGraphsSlot.captured.values.first()
            assertTrue(
                serviceGraph.contains("Eksempeltjeneste"),
                "produced graph should contain the public service title",
            )
            assertTrue(
                serviceGraph.contains("PublicService"),
                "produced graph should type the resource as a public service",
            )
            assertTrue(
                serviceGraph.contains("CatalogRecord") && serviceGraph.contains(serviceUri),
                "produced graph should contain a catalog record with the service as primary topic",
            )
            assertTrue(
                serviceGraph.contains("$serviceUri/output/0") && serviceGraph.contains("Produserer-tittel"),
                "produced graph should inline the produced output with its title",
            )
            assertTrue(
                serviceGraph.contains("$serviceUri/evidence/0") && serviceGraph.contains("$serviceUri/evidence/1"),
                "produced graph should contain both required evidence references",
            )
            assertTrue(
                serviceGraph.contains("Navn på dokumentasjonskrav") &&
                    serviceGraph.contains("Name") &&
                    serviceGraph.contains("dokumentasjon 2"),
                "produced graph should inline the required evidence titles",
            )
        } finally {
            server.stop()
        }
    }

    @Test
    fun `service harvester - generated catalog path with free service and publisher`() {
        val ttl =
            """
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
            every { orgAdapter.getOrganization("123") } returns
                Organization(
                    organizationId = "123",
                    uri = "http://example.org/org",
                    name = "Org",
                    prefLabel = PrefLabel(nb = "NB", nn = "NN", en = "EN"),
                )
            val resourceRepository = mockResourceRepository()
            val harvestSourceRepository = mockHarvestSourceRepository()
            val producer = mockk<ResourceEventProducer>(relaxed = true)

            val appProps =
                ApplicationProperties(
                    serviceUri = "https://services.fellesdatakatalog.digdir.no/services",
                )
            val harvester = ServiceHarvester(appProps, orgAdapter, resourceRepository, harvestSourceRepository, producer)
            val report =
                harvester.harvestServices(
                    source =
                        HarvestDataSource(
                            id = "source-1",
                            url = "http://localhost:${server.port()}/rdf",
                            acceptHeaderValue = "text/turtle",
                            publisherId = "123",
                        ),
                    harvestDate = Calendar.getInstance(),
                    forceUpdate = false,
                    runId = "run-1",
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
        val ttl =
            """
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

            val appProps =
                ApplicationProperties(
                    eventUri = "https://events.fellesdatakatalog.digdir.no/events",
                )
            val harvester = EventHarvester(appProps, orgAdapter, resourceRepository, harvestSourceRepository, producer)
            val report =
                harvester.harvestEvents(
                    source =
                        HarvestDataSource(
                            id = "source-1",
                            url = "http://localhost:${server.port()}/rdf",
                            acceptHeaderValue = "text/turtle",
                            publisherId = null,
                        ),
                    harvestDate = Calendar.getInstance(),
                    forceUpdate = false,
                    runId = "run-1",
                )

            assertNotNull(report)
            assertFalse(report!!.harvestError)
            verify(atLeast = 1) { resourceRepository.save(any<ResourceEntity>()) }
        } finally {
            server.stop()
        }
    }

    @Test
    fun `dataset harvester - marks previously harvested datasets as removed when absent from new harvest`() {
        val ttl =
            """
            @prefix dcat: <http://www.w3.org/ns/dcat#> .
            @prefix dct: <http://purl.org/dc/terms/> .
            <http://example.org/catalog> a dcat:Catalog ;
              dcat:dataset <http://example.org/dataset1> .
            <http://example.org/dataset1> a dcat:Dataset ; dct:title "Dataset 1" .
            """.trimIndent()

        val server = wireMock(ttl)
        try {
            val harvestSourceEntity =
                HarvestSourceEntity(
                    id = 1L,
                    uri = "http://localhost:${server.port()}/rdf",
                    checksum = "old",
                    issued = Instant.now(),
                )
            val staleDataset =
                ResourceEntity(
                    uri = "http://example.org/old-dataset",
                    type = ResourceType.DATASET,
                    fdkId = "fdk-old",
                    removed = false,
                    issued = Instant.now(),
                    modified = Instant.now(),
                    checksum = "stale",
                    harvestSource = harvestSourceEntity,
                )

            val resourceRepository = mockk<ResourceRepository>()
            every { resourceRepository.findById(any<String>()) } returns Optional.empty()
            every { resourceRepository.findAllByType(ResourceType.DATASET) } returns listOf(staleDataset)
            every { resourceRepository.findAllByTypeAndRemoved(any(), any()) } returns emptyList()
            every { resourceRepository.findByFdkId(any()) } returns null
            every { resourceRepository.findAllByFdkId(any()) } returns emptyList()
            every { resourceRepository.save(any<ResourceEntity>()) } answers { firstArg() }
            every { resourceRepository.saveAll(any<Iterable<ResourceEntity>>()) } answers { firstArg<Iterable<ResourceEntity>>().toList() }

            val harvestSourceRepository = mockk<HarvestSourceRepository>()
            every { harvestSourceRepository.findByUri(any()) } returns null
            every { harvestSourceRepository.save(any()) } answers { firstArg<HarvestSourceEntity>().copy(id = 1L) }

            val producer = mockk<ResourceEventProducer>(relaxed = true)

            val appProps = ApplicationProperties(datasetUri = "https://datasets.fellesdatakatalog.digdir.no/datasets")
            val harvester = DatasetHarvester(appProps, mockk(relaxed = true), resourceRepository, harvestSourceRepository, producer)
            val report =
                harvester.harvestDatasetCatalog(
                    source =
                        HarvestDataSource(
                            id = "source-1",
                            url = "http://localhost:${server.port()}/rdf",
                            acceptHeaderValue = "text/turtle",
                        ),
                    harvestDate = Calendar.getInstance(),
                    forceUpdate = false,
                    runId = "run-1",
                )

            assertNotNull(report)
            assertFalse(report!!.harvestError)
            assertEquals(1, report.removedResources.size, "stale dataset should be reported as removed")
            assertEquals("http://example.org/old-dataset", report.removedResources.first().uri)
            verify(exactly = 1) {
                producer.publishRemovedEvents(
                    dataType = DataType.dataset,
                    resources = match { it.any { r -> r.uri == "http://example.org/old-dataset" } },
                    runId = "run-1",
                )
            }
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
                        .withBody(turtle),
                ),
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

    private fun mockResourceRepositoryWithConflicts(conflicts: Map<String, ResourceType>): ResourceRepository {
        val repo = mockk<ResourceRepository>()
        every { repo.findById(any<String>()) } answers {
            val uri = firstArg<String>()
            val conflictType = conflicts[uri]
            if (conflictType == null) {
                Optional.empty()
            } else {
                Optional.of(
                    ResourceEntity(
                        uri = uri,
                        type = conflictType,
                        fdkId = "existing-fdk-id",
                        removed = false,
                        issued = Instant.now(),
                        modified = Instant.now(),
                        checksum = "existing-checksum",
                        harvestSource =
                            HarvestSourceEntity(
                                id = 2L,
                                uri = "http://example.org/other-source",
                                checksum = "other-source-checksum",
                                issued = Instant.now(),
                            ),
                    ),
                )
            }
        }
        every { repo.findAllByType(any()) } returns emptyList()
        every { repo.findAllByTypeAndRemoved(any(), any()) } returns emptyList()
        every { repo.findByFdkId(any()) } returns null
        every { repo.findAllByFdkId(any()) } returns emptyList()
        every { repo.save(any<ResourceEntity>()) } answers { firstArg() }
        every { repo.saveAll(any<Iterable<ResourceEntity>>()) } answers { firstArg<Iterable<ResourceEntity>>().toList() }
        return repo
    }
}
