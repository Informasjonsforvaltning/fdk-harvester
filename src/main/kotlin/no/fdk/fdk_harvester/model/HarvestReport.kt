package no.fdk.fdk_harvester.model

import com.fasterxml.jackson.annotation.JsonIgnoreProperties

/**
 * Result of a harvest or delete operation: run id, source id/url, data type, timestamps,
 * and lists of changed/removed resources (as [FdkIdAndUri]).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
data class HarvestReport(
    val runId: String,
    val dataSourceId: String,
    val dataSourceUrl: String,
    val dataType: String,
    val harvestError: Boolean,
    val startTime: String,
    val endTime: String,
    val errorMessage: String? = null,
    val changedCatalogs: List<FdkIdAndUri> = emptyList(),
    val changedResources: List<FdkIdAndUri> = emptyList(),
    val removedResources: List<FdkIdAndUri> = emptyList()
)

/**
 * FDK identifier and URI for a harvested or removed resource.
 */
data class FdkIdAndUri(
    val fdkId: String,
    val uri: String
)

