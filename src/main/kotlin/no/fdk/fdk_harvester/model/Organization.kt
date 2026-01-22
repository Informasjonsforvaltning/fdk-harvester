package no.fdk.fdk_harvester.model

import com.fasterxml.jackson.annotation.JsonIgnoreProperties

@JsonIgnoreProperties(ignoreUnknown = true)
data class Organization(
    val organizationId: String? = null,
    val uri: String? = null,
    val name: String? = null,
    val prefLabel: PrefLabel? = null,
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class PrefLabel(
    val nb: String? = null,
    val nn: String? = null,
    val en: String? = null
)

