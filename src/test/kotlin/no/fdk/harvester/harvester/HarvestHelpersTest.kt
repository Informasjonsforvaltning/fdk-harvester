package no.fdk.harvester.harvester

import org.apache.jena.rdf.model.ModelFactory
import org.apache.jena.vocabulary.RDF
import org.apache.jena.vocabulary.SKOS
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test

@Tag("unit")
class HarvestHelpersTest {
    @Test
    fun `Statement_isResourceProperty returns false for literal object`() {
        val m = ModelFactory.createDefaultModel()
        val s = m.createResource("http://example.org/s")
        val p = m.createProperty("http://example.org/p")
        val stmt = m.createStatement(s, p, m.createLiteral("x"))

        assertFalse(stmt.isResourceProperty())
    }

    @Test
    fun `excludeBlankNodes drops blank nodes and keeps URI resources`() {
        val m = ModelFactory.createDefaultModel()
        val uriResource = m.createResource("http://example.org/concept1")
        uriResource.addProperty(RDF.type, SKOS.Concept)
        val blankNode = m.createResource()
        blankNode.addProperty(RDF.type, SKOS.Concept)

        val filtered =
            listOf(uriResource, blankNode).excludeBlankNodes("http://example.org/source")

        assertEquals(1, filtered.size)
        assertEquals("http://example.org/concept1", filtered.first().uri)
    }
}
