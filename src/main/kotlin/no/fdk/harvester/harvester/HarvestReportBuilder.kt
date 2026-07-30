package no.fdk.harvester.harvester

import no.fdk.harvester.error.HarvestErrorCategory
import no.fdk.harvester.error.HarvestErrorMessageMapper
import no.fdk.harvester.model.FdkIdAndUri
import no.fdk.harvester.model.HarvestDataSource
import no.fdk.harvester.model.HarvestReport
import java.util.Calendar

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
        runId: String,
    ): HarvestReport =
        HarvestReport(
            runId = runId,
            dataSourceId = sourceId,
            dataSourceUrl = sourceUrl,
            dataType = dataType,
            harvestError = false,
            startTime = harvestDate.formatWithOsloTimeZone(),
            endTime = formatNowWithOsloTimeZone(),
            changedCatalogs = changedCatalogs,
            changedResources = changedResources,
            removedResources = removedResources,
        )

    /** Creates a report when no resources were changed or removed. */
    fun createNoChangeReport(
        dataType: String,
        sourceId: String,
        sourceUrl: String,
        harvestDate: Calendar,
        runId: String,
    ): HarvestReport =
        HarvestReport(
            runId = runId,
            dataSourceId = sourceId,
            dataSourceUrl = sourceUrl,
            dataType = dataType,
            harvestError = false,
            startTime = harvestDate.formatWithOsloTimeZone(),
            endTime = formatNowWithOsloTimeZone(),
        )

    /** Creates a report for a failed harvest with [errorMessage]. */
    fun createErrorReport(
        dataType: String,
        source: HarvestDataSource,
        errorMessage: String,
        errorCategory: HarvestErrorCategory,
        harvestDate: Calendar,
        runId: String,
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
            errorCategory = errorCategory,
            startTime = harvestDate.formatWithOsloTimeZone(),
            endTime = formatNowWithOsloTimeZone(),
        )
    }

    /** Creates a validation error report when required source fields are missing. */
    fun createValidationErrorReport(
        dataType: String,
        runId: String,
        harvestDate: Calendar,
        sourceId: String?,
        sourceUrl: String?,
    ): HarvestReport =
        HarvestReport(
            runId = runId,
            dataSourceId = sourceId ?: "",
            dataSourceUrl = sourceUrl,
            dataType = dataType,
            harvestError = true,
            errorMessage =
                HarvestErrorMessageMapper.toUserMessage(
                    category = HarvestErrorCategory.VALIDATION_ERROR,
                    dataSourceUrl = sourceUrl,
                    dataType = null,
                ),
            errorCategory = HarvestErrorCategory.VALIDATION_ERROR,
            startTime = harvestDate.formatWithOsloTimeZone(),
            endTime = formatNowWithOsloTimeZone(),
        )
}
