package no.fdk.fdk_harvester.harvester

import no.fdk.fdk_harvester.model.FdkIdAndUri
import no.fdk.fdk_harvester.model.HarvestDataSource
import no.fdk.fdk_harvester.model.HarvestReport
import java.util.*

/** Builds [HarvestReport] instances for success, no-change, and error outcomes. */
object HarvestReportBuilder {
    /** Creates a success report with changed/removed resource lists. */
    fun createSuccessReport(
        dataType: String,
        sourceId: String,
        sourceUrl: String?,
        harvestDate: Calendar,
        changedCatalogs: List<FdkIdAndUri>,
        changedResources: List<FdkIdAndUri>,
        removedResources: List<FdkIdAndUri>,
        runId: String
    ): HarvestReport {
        return HarvestReport(
            runId = runId,
            dataSourceId = sourceId,
            dataSourceUrl = sourceUrl,
            dataType = dataType,
            harvestError = false,
            startTime = harvestDate.formatWithOsloTimeZone(),
            endTime = formatNowWithOsloTimeZone(),
            changedCatalogs = changedCatalogs,
            changedResources = changedResources,
            removedResources = removedResources
        )
    }

    /** Creates a report when no resources were changed or removed. */
    fun createNoChangeReport(
        dataType: String,
        sourceId: String,
        sourceUrl: String,
        harvestDate: Calendar,
        runId: String
    ): HarvestReport {
        return HarvestReport(
            runId = runId,
            dataSourceId = sourceId,
            dataSourceUrl = sourceUrl,
            dataType = dataType,
            harvestError = false,
            startTime = harvestDate.formatWithOsloTimeZone(),
            endTime = formatNowWithOsloTimeZone()
        )
    }

    /** Creates a report for a failed harvest with [errorMessage]. */
    fun createErrorReport(
        dataType: String,
        source: HarvestDataSource,
        errorMessage: String,
        harvestDate: Calendar,
        runId: String
    ): HarvestReport {
        val sourceId = requireNotNull(source.id) { "HarvestDataSource missing id" }
        val sourceUrl = requireNotNull(source.url) { "HarvestDataSource missing url" }
        return HarvestReport(
            runId = runId,
            dataSourceId = sourceId,
            dataSourceUrl = sourceUrl,
            dataType = dataType,
            harvestError = true,
            errorMessage = errorMessage,
            startTime = harvestDate.formatWithOsloTimeZone(),
            endTime = formatNowWithOsloTimeZone()
        )
    }
}



