package no.fdk.harvester.model

data class SourceLookupRequest(val fdkIds: List<String> = emptyList())

data class SourceLookupResponse(val sources: List<SourceWithFdkIds>, val notFound: List<String>)

data class SourceWithFdkIds(val dataSourceUrl: String, val resourceType: ResourceType, val fdkIds: List<String>)
