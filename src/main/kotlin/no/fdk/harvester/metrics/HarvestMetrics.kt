package no.fdk.harvester.metrics

import io.micrometer.core.instrument.Metrics
import no.fdk.harvest.DataType
import no.fdk.harvester.error.HarvestErrorCategory
import no.fdk.harvester.model.HarvestReport
import kotlin.time.Duration
import kotlin.time.toJavaDuration

object HarvestMetrics {
    fun record(
        report: HarvestReport?,
        dataType: DataType,
        forceUpdate: Boolean,
        dataSourceId: String,
        dataSourceUrl: String,
        duration: Duration,
    ) {
        val type = metricType(dataType)
        val forceUpdateLabel = "$forceUpdate"
        val success = report?.harvestError == false

        Metrics
            .counter(
                "harvest_count",
                "status",
                if (success) "success" else "error",
                "type",
                type,
                "force_update",
                forceUpdateLabel,
                "datasource_id",
                dataSourceId,
                "datasource_url",
                dataSourceUrl,
            ).increment()

        if (!success) {
            recordErrorCount(
                category = report?.errorCategory ?: HarvestErrorCategory.INTERNAL_ERROR,
                dataType = dataType,
            )
        }

        if (success && report != null) {
            Metrics
                .counter(
                    "harvest_changed_resources_count",
                    "type",
                    type,
                    "force_update",
                    forceUpdateLabel,
                    "datasource_id",
                    dataSourceId,
                    "datasource_url",
                    dataSourceUrl,
                ).increment(report.changedResources.size.toDouble())
            Metrics
                .counter(
                    "harvest_removed_resources_count",
                    "type",
                    type,
                    "force_update",
                    forceUpdateLabel,
                    "datasource_id",
                    dataSourceId,
                    "datasource_url",
                    dataSourceUrl,
                ).increment(report.removedResources.size.toDouble())
            Metrics
                .timer(
                    "harvest_time",
                    "type",
                    type,
                    "force_update",
                    forceUpdateLabel,
                    "datasource_id",
                    dataSourceId,
                    "datasource_url",
                    dataSourceUrl,
                ).record(duration.toJavaDuration())
        }
    }

    fun recordError(
        dataType: DataType,
        forceUpdate: Boolean,
        dataSourceId: String?,
        dataSourceUrl: String?,
        category: HarvestErrorCategory,
    ) {
        Metrics
            .counter(
                "harvest_count",
                "status",
                "error",
                "type",
                metricType(dataType),
                "force_update",
                "$forceUpdate",
                "datasource_id",
                dataSourceId ?: "",
                "datasource_url",
                dataSourceUrl ?: "",
            ).increment()
        recordErrorCount(category = category, dataType = dataType)
    }

    fun recordErrorCount(
        category: HarvestErrorCategory,
        dataType: DataType,
    ) {
        Metrics
            .counter(
                "harvest_error_count",
                "category",
                category.name.lowercase(),
                "type",
                metricType(dataType),
            ).increment()
    }

    fun metricType(dataType: DataType): String =
        when (dataType) {
            DataType.informationmodel -> "information-model"
            DataType.publicService, DataType.service -> "public-service"
            else -> dataType.name
        }
}
