package no.fdk.fdk_harvester.harvester

import no.fdk.fdk_harvester.model.FdkIdAndUri
import no.fdk.fdk_harvester.model.HarvestDataSource
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import java.util.*

@Tag("unit")
class HarvestReportBuilderTest {

    @Test
    fun `test createSuccessReport`() {
        val harvestDate = Calendar.getInstance()
        val changedCatalogs = listOf(FdkIdAndUri("catalog-1", "http://example.org/catalog1"))
        val changedResources = listOf(FdkIdAndUri("resource-1", "http://example.org/resource1"))
        val removedResources = listOf(FdkIdAndUri("resource-2", "http://example.org/resource2"))

        val report = HarvestReportBuilder.createSuccessReport(
            dataType = "dataset",
            sourceId = "source-1",
            sourceUrl = "http://example.org/source",
            harvestDate = harvestDate,
            changedCatalogs = changedCatalogs,
            changedResources = changedResources,
            removedResources = removedResources,
            runId = "run-123"
        )

        assertEquals("run-123", report.runId)
        assertEquals("source-1", report.dataSourceId)
        assertEquals("http://example.org/source", report.dataSourceUrl)
        assertEquals("dataset", report.dataType)
        assertFalse(report.harvestError)
        assertNull(report.errorMessage)
        assertEquals(changedCatalogs, report.changedCatalogs)
        assertEquals(changedResources, report.changedResources)
        assertEquals(removedResources, report.removedResources)
        assertNotNull(report.startTime)
        assertNotNull(report.endTime)
    }

    @Test
    fun `test createNoChangeReport`() {
        val harvestDate = Calendar.getInstance()

        val report = HarvestReportBuilder.createNoChangeReport(
            dataType = "concept",
            sourceId = "source-1",
            sourceUrl = "http://example.org/source",
            harvestDate = harvestDate,
            runId = "run-123"
        )

        assertEquals("run-123", report.runId)
        assertEquals("source-1", report.dataSourceId)
        assertEquals("http://example.org/source", report.dataSourceUrl)
        assertEquals("concept", report.dataType)
        assertFalse(report.harvestError)
        assertNull(report.errorMessage)
        assertTrue(report.changedCatalogs.isEmpty())
        assertTrue(report.changedResources.isEmpty())
        assertTrue(report.removedResources.isEmpty())
        assertNotNull(report.startTime)
        assertNotNull(report.endTime)
    }

    @Test
    fun `test createErrorReport`() {
        val harvestDate = Calendar.getInstance()
        val source = HarvestDataSource(
            id = "source-1",
            url = "http://example.org/source",
            acceptHeaderValue = "text/turtle"
        )

        val report = HarvestReportBuilder.createErrorReport(
            dataType = "event",
            source = source,
            errorMessage = "Test error",
            harvestDate = harvestDate,
            runId = "run-123"
        )

        assertEquals("run-123", report.runId)
        assertEquals("source-1", report.dataSourceId)
        assertEquals("http://example.org/source", report.dataSourceUrl)
        assertEquals("event", report.dataType)
        assertTrue(report.harvestError)
        assertEquals("Test error", report.errorMessage)
        assertTrue(report.changedCatalogs.isEmpty())
        assertTrue(report.changedResources.isEmpty())
        assertTrue(report.removedResources.isEmpty())
        assertNotNull(report.startTime)
        assertNotNull(report.endTime)
    }

    @Test
    fun `test createSuccessReport with empty lists`() {
        val harvestDate = Calendar.getInstance()

        val report = HarvestReportBuilder.createSuccessReport(
            dataType = "dataset",
            sourceId = "source-1",
            sourceUrl = "http://example.org/source",
            harvestDate = harvestDate,
            changedCatalogs = emptyList(),
            changedResources = emptyList(),
            removedResources = emptyList(),
            runId = "run-id-456"
        )

        assertEquals("run-id-456", report.runId)
        assertTrue(report.changedCatalogs.isEmpty())
        assertTrue(report.changedResources.isEmpty())
        assertTrue(report.removedResources.isEmpty())
    }
}

