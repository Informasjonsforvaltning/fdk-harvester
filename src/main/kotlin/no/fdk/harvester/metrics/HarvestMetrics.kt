package no.fdk.harvester.metrics

import io.micrometer.core.instrument.Metrics
import no.fdk.harvest.DataType
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
    }

    fun metricType(dataType: DataType): String =
        when (dataType) {
            DataType.informationmodel -> "information-model"
            DataType.publicService, DataType.service -> "public-service"
            else -> dataType.name
        }
}
