package no.fdk.fdk_harvester.model

data class DuplicateIRI(
    val iriToRemove: String,
    val iriToRetain: String,
    val keepRemovedFdkId: Boolean = false
)

