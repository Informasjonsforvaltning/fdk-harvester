package no.fdk.harvester.model

data class DuplicateIRI(
    val iriToRemove: String,
    val iriToRetain: String,
    val keepRemovedFdkId: Boolean = false,
)
