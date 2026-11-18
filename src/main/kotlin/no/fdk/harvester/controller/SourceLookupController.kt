package no.fdk.harvester.controller

import no.fdk.harvester.model.SourceLookupRequest
import no.fdk.harvester.model.SourceLookupResponse
import no.fdk.harvester.model.SourceWithFdkIds
import no.fdk.harvester.repository.ResourceRepository
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RestController

@RestController
class SourceLookupController(private val resourceRepository: ResourceRepository) {
    @PostMapping("/sources/lookup")
    fun lookupSources(@RequestBody request: SourceLookupRequest): ResponseEntity<SourceLookupResponse> {
        val fdkIds = request.fdkIds.toSet()
        if (fdkIds.isEmpty() || fdkIds.size > MAX_FDK_IDS) {
            return ResponseEntity.badRequest().build()
        }

        val found = resourceRepository.findSourcesByFdkIds(fdkIds)
        val sources =
            found
                .groupBy { it.sourceUrl to it.type }
                .map { (key, resources) ->
                    SourceWithFdkIds(
                        dataSourceUrl = key.first,
                        resourceType = key.second,
                        fdkIds = resources.map { it.fdkId }.distinct().sorted(),
                    )
                }.sortedBy { it.dataSourceUrl }
        val notFound = (fdkIds - found.map { it.fdkId }.toSet()).sorted()

        return ResponseEntity.ok(SourceLookupResponse(sources = sources, notFound = notFound))
    }

    companion object {
        private const val MAX_FDK_IDS = 1000
    }
}
