package no.fdk.fdk_harvester.harvester

import no.fdk.fdk_harvester.model.Organization
import no.fdk.fdk_harvester.model.PrefLabel
import org.apache.jena.rdf.model.ModelFactory
import org.apache.jena.vocabulary.RDF
import org.apache.jena.vocabulary.SKOS
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
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
    fun `splitConceptsFromRDF filters blank node concepts`() {
        val m = ModelFactory.createDefaultModel()
        val bn = m.createResource() // blank node
        bn.addProperty(RDF.type, SKOS.Concept)

        val concepts = splitConceptsFromRDF(m, "http://example.org/source")
        assertTrue(concepts.isEmpty())
    }

    @Test
    fun `splitCollectionsFromRDF generates a collection when there are free concepts and organization labels are used`() {
        val m = ModelFactory.createDefaultModel()

        // one concept not member of any collection
        val concept = m.createResource("http://example.org/concept1")
        concept.addProperty(RDF.type, SKOS.Concept)

        val concepts = splitConceptsFromRDF(m, "http://example.org/source")
        val org = Organization(
            organizationId = "1",
            uri = "http://example.org/org",
            name = "OrgName",
            prefLabel = PrefLabel(nb = "NB", nn = "NN", en = "EN")
        )

        val collections = splitCollectionsFromRDF(
            harvested = m,
            allConcepts = concepts,
            sourceURL = "http://example.org/source",
            organization = org
        )

        // generated collection should be present
        assertTrue(collections.any { it.resourceURI == "http://example.org/source#GeneratedCollection" })
    }

    @Test
    fun `containsConceptsWithoutCollection detects free concepts`() {
        val m = ModelFactory.createDefaultModel()
        val concept = m.createResource("http://example.org/concept1")
        concept.addProperty(RDF.type, SKOS.Concept)

        val concepts = splitConceptsFromRDF(m, "http://example.org/source")
        assertTrue(concepts.containsConceptsWithoutCollection())
        assertEquals(1, concepts.size)
    }
}




