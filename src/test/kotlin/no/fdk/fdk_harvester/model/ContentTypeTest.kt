package no.fdk.fdk_harvester.model

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test

@Tag("unit")
class ContentTypeTest {

    @Test
    fun `test all content types are RDF`() {
        ContentType.values().forEach { contentType ->
            assertTrue(contentType.isRDF(), "$contentType should be RDF")
        }
    }

    @Test
    fun `test fromValue with turtle`() {
        assertEquals(ContentType.TURTLE, ContentType.fromValue("text/turtle"))
        assertEquals(ContentType.TURTLE, ContentType.fromValue("application/turtle"))
    }

    @Test
    fun `test fromValue with JSONLD`() {
        assertEquals(ContentType.JSONLD, ContentType.fromValue("application/ld+json"))
    }

    @Test
    fun `test fromValue with RDFXML`() {
        assertEquals(ContentType.RDFXML, ContentType.fromValue("application/rdf+xml"))
    }

    @Test
    fun `test fromValue with null`() {
        assertNull(ContentType.fromValue(null))
    }

    @Test
    fun `test fromValue with unknown`() {
        assertNull(ContentType.fromValue("unknown/type"))
    }

    @Test
    fun `test fromValue is case insensitive`() {
        assertEquals(ContentType.TURTLE, ContentType.fromValue("TEXT/TURTLE"))
        assertEquals(ContentType.JSONLD, ContentType.fromValue("APPLICATION/LD+JSON"))
    }

    @Test
    fun `test N3 headers`() {
        assertTrue(ContentType.N3.headers.contains("text/rdf+n3"))
        assertTrue(ContentType.N3.headers.contains("application/n3"))
        assertTrue(ContentType.N3.headers.contains("text/n3"))
    }

    @Test
    fun `test NTRIPLES headers`() {
        assertTrue(ContentType.NTRIPLES.headers.contains("application/n-triples"))
        assertTrue(ContentType.NTRIPLES.headers.contains("text/plain"))
    }
}

