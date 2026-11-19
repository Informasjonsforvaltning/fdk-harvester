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

    @Test
    fun `rejects more than 1000 ids`() {
        val response = controller.lookupSources(SourceLookupRequest((1..1001).map { "id-$it" }))

        assertEquals(HttpStatus.BAD_REQUEST, response.statusCode)
        verify(exactly = 0) { resourceRepository.findSourcesByFdkIds(any()) }
    }

    @Test
    fun `accepts 1000 ids`() {
        every { resourceRepository.findSourcesByFdkIds(any()) } returns emptyList()

        val response = controller.lookupSources(SourceLookupRequest((1..1000).map { "id-$it" }))

        assertEquals(HttpStatus.OK, response.statusCode)
    }

    @Test
    fun `splits ids from same source by resource type`() {
        every { resourceRepository.findSourcesByFdkIds(any()) } returns
            listOf(
                source("dataset", "http://example.org/a"),
                source("dataservice", "http://example.org/a", ResourceType.DATASERVICE),
            )

        val response = controller.lookupSources(SourceLookupRequest(listOf("dataset", "dataservice")))

        assertEquals(
            setOf(
                SourceWithFdkIds("http://example.org/a", ResourceType.DATASET, listOf("dataset")),
                SourceWithFdkIds("http://example.org/a", ResourceType.DATASERVICE, listOf("dataservice")),
            ),
            response.body!!.sources.toSet(),
        )
        assertEquals(emptyList<String>(), response.body!!.notFound)
    }

    private fun source(fdkId: String, sourceUrl: String, type: ResourceType = ResourceType.DATASET): ResourceSource =
        object : ResourceSource {
            override val fdkId = fdkId
            override val type = type
            override val sourceUrl = sourceUrl
        }
}
