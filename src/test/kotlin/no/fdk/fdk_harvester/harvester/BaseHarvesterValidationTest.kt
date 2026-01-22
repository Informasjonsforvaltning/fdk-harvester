package no.fdk.fdk_harvester.harvester

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock.aResponse
import com.github.tomakehurst.wiremock.client.WireMock.get
import com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo
import com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import io.mockk.every
import io.mockk.mockk
import no.fdk.fdk_harvester.model.HarvestDataSource
import no.fdk.fdk_harvester.model.HarvestReport
import no.fdk.fdk_harvester.model.HarvestSourceEntity
import no.fdk.fdk_harvester.repository.HarvestSourceRepository
import org.apache.jena.rdf.model.Model
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import java.util.Calendar

@Tag("unit")
class BaseHarvesterValidationTest {

    @Test
    fun `invalid source without id or url returns null`() {
        val repo = mockk<HarvestSourceRepository>()
        val harvester = TestHarvester(repo)

        val report = harvester.harvest(
            source = HarvestDataSource(id = null, url = null, acceptHeaderValue = "text/turtle"),
            harvestDate = Calendar.getInstance()
        )

        assertNull(report)
    }

    @Test
    fun `missing accept header returns null when required`() {
        val repo = mockk<HarvestSourceRepository>()
        val harvester = TestHarvester(repo)

        val report = harvester.harvest(
            source = HarvestDataSource(id = "s1", url = "http://example.org", acceptHeaderValue = null),
            harvestDate = Calendar.getInstance()
        )

        assertNull(report)
    }

    @Test
    fun `wildcard accept header yields error report`() {
        val server = WireMockServer(wireMockConfig().dynamicPort())
        server.start()
        try {
            server.stubFor(get(urlEqualTo("/rdf")).willReturn(aResponse().withStatus(200).withBody("x")))

            val repo = mockk<HarvestSourceRepository>()
            val harvester = TestHarvester(repo)

            val report = harvester.harvest(
                source = HarvestDataSource(
                    id = "s1",
                    url = "http://localhost:${server.port()}/rdf",
                    acceptHeaderValue = "*/*"
                ),
                harvestDate = Calendar.getInstance()
            )

            assertNotNull(report)
            assertTrue(report!!.harvestError)
        } finally {
            server.stop()
        }
    }

    @Test
    fun `unacceptable accept header yields error report`() {
        val server = WireMockServer(wireMockConfig().dynamicPort())
        server.start()
        try {
            server.stubFor(get(urlEqualTo("/rdf")).willReturn(aResponse().withStatus(200).withBody("x")))

            val repo = mockk<HarvestSourceRepository>()
            val harvester = TestHarvester(repo)

            val report = harvester.harvest(
                source = HarvestDataSource(
                    id = "s1",
                    url = "http://localhost:${server.port()}/rdf",
                    acceptHeaderValue = "application/unknown"
                ),
                harvestDate = Calendar.getInstance()
            )

            assertNotNull(report)
            assertTrue(report!!.harvestError)
        } finally {
            server.stop()
        }
    }

    @Test
    fun `http error during fetch yields error report`() {
        val server = WireMockServer(wireMockConfig().dynamicPort())
        server.start()
        try {
            server.stubFor(get(urlEqualTo("/rdf")).willReturn(aResponse().withStatus(500).withBody("nope")))

            val repo = mockk<HarvestSourceRepository>()
            val harvester = TestHarvester(repo)

            val report = harvester.harvest(
                source = HarvestDataSource(
                    id = "s1",
                    url = "http://localhost:${server.port()}/rdf",
                    acceptHeaderValue = "text/turtle"
                ),
                harvestDate = Calendar.getInstance()
            )

            assertNotNull(report)
            assertTrue(report!!.harvestError)
        } finally {
            server.stop()
        }
    }

    @Test
    fun `successful harvest parses rdf and calls updateDB`() {
        val ttl = """
            @prefix ex: <http://example.org/> .
            ex:s ex:p ex:o .
        """.trimIndent()

        val server = WireMockServer(wireMockConfig().dynamicPort())
        server.start()
        try {
            server.stubFor(
                get(urlPathEqualTo("/rdf"))
                    .willReturn(
                        aResponse()
                            .withStatus(200)
                            .withHeader("Content-Type", "text/turtle")
                            .withBody(ttl)
                    )
            )

            val repo = mockk<HarvestSourceRepository>()
            every { repo.findByUri(any()) } returns null
            every { repo.save(any()) } answers { firstArg<HarvestSourceEntity>().copy(id = 1L) }

            val harvester = TestHarvester(repo)
            val report = harvester.harvest(
                source = HarvestDataSource(
                    id = "s1",
                    url = "http://localhost:${server.port()}/rdf",
                    acceptHeaderValue = "text/turtle"
                ),
                harvestDate = Calendar.getInstance()
            )

            assertNotNull(report)
            assertFalse(report!!.harvestError)
        } finally {
            server.stop()
        }
    }

    private class TestHarvester(
        harvestSourceRepository: HarvestSourceRepository
    ) : BaseHarvester(harvestSourceRepository) {
        fun harvest(source: HarvestDataSource, harvestDate: Calendar): HarvestReport? =
            validateAndHarvest(source, harvestDate, forceUpdate = false, runId = "run-1", dataType = "test", requiresAcceptHeader = true)

        override fun updateDB(
            harvested: Model,
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
    }
}


