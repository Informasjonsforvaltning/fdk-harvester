package no.fdk.harvester.controller

import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import no.fdk.harvester.model.ResourceType
import no.fdk.harvester.model.SourceLookupRequest
import no.fdk.harvester.model.SourceWithFdkIds
import no.fdk.harvester.repository.ResourceRepository
import no.fdk.harvester.repository.ResourceSource
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.springframework.http.HttpStatus

@Tag("unit")
class SourceLookupControllerTest {
    private val resourceRepository = mockk<ResourceRepository>()
    private val controller = SourceLookupController(resourceRepository)

    @Test
    fun `groups fdk ids by source and lists unknown ids`() {
        every { resourceRepository.findSourcesByFdkIds(any()) } returns
            listOf(
                source("b1", "http://example.org/b"),
                source("a1", "http://example.org/a"),
                source("a2", "http://example.org/a"),
            )

        val response = controller.lookupSources(SourceLookupRequest(listOf("a2", "a1", "b1", "unknown", "a1")))

        assertEquals(HttpStatus.OK, response.statusCode)
        assertEquals(
            listOf(
                SourceWithFdkIds("http://example.org/a", ResourceType.DATASET, listOf("a1", "a2")),
                SourceWithFdkIds("http://example.org/b", ResourceType.DATASET, listOf("b1")),
            ),
            response.body!!.sources,
        )
        assertEquals(listOf("unknown"), response.body!!.notFound)
        verify { resourceRepository.findSourcesByFdkIds(setOf("a2", "a1", "b1", "unknown")) }
    }

    @Test
    fun `rejects empty request`() {
        val response = controller.lookupSources(SourceLookupRequest(emptyList()))

        assertEquals(HttpStatus.BAD_REQUEST, response.statusCode)
        verify(exactly = 0) { resourceRepository.findSourcesByFdkIds(any()) }
    }

    private fun source(fdkId: String, sourceUrl: String): ResourceSource = object : ResourceSource {
        override val fdkId = fdkId
        override val type = ResourceType.DATASET
        override val sourceUrl = sourceUrl
    }
}
