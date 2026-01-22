package no.fdk.fdk_harvester.model

import com.fasterxml.jackson.annotation.JsonIgnoreProperties

/**
 * Configuration for a harvest source: URL, accept header, optional auth, and data type.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
data class HarvestDataSource(
    val id: String? = null,
    val dataSourceType: String? = null,
    val dataType: String? = null,
    val url: String? = null,
    val acceptHeaderValue: String? = null,
    val publisherId: String? = null,
    val authHeader: AuthHeader? = null
)

/** Optional HTTP auth header (type and value) for harvesting protected sources. */
@JsonIgnoreProperties(ignoreUnknown = true)
data class AuthHeader(
    val name: String? = null,
    val type: String? = null,
    val value: String? = null
)

