package no.fdk.harvester.rdf

import org.apache.jena.rdf.model.ModelFactory
import org.apache.jena.riot.Lang
import org.apache.jena.sparql.vocabulary.FOAF
import org.apache.jena.vocabulary.DCAT
import org.apache.jena.vocabulary.DCTerms
import org.apache.jena.vocabulary.RDF
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import java.time.Instant

@Tag("unit")
class RDFUtilsTest {
    @Test
    fun `test jenaTypeFromAcceptHeader with turtle`() {
        assertEquals(Lang.TURTLE, jenaTypeFromAcceptHeader("text/turtle"))
        assertEquals(Lang.TURTLE, jenaTypeFromAcceptHeader("application/turtle"))
    }

    @Test
    fun `test jenaTypeFromAcceptHeader with RDFXML`() {
        assertEquals(Lang.RDFXML, jenaTypeFromAcceptHeader("application/rdf+xml"))
    }

    @Test
    fun `test jenaTypeFromAcceptHeader with JSONLD`() {
        assertEquals(Lang.JSONLD, jenaTypeFromAcceptHeader("application/ld+json"))
    }

    @Test
    fun `test jenaTypeFromAcceptHeader with null`() {
        assertNull(jenaTypeFromAcceptHeader(null))
    }

    @Test
    fun `test jenaTypeFromAcceptHeader with wildcard`() {
        assertNull(jenaTypeFromAcceptHeader("*/*"))
    }

    @Test
    fun `test jenaTypeFromAcceptHeader with unknown`() {
        assertEquals(Lang.RDFNULL, jenaTypeFromAcceptHeader("unknown/type"))
    }

    @Test
    fun `test computeChecksum produces consistent results`() {
        val model1 = ModelFactory.createDefaultModel()
        model1
            .createResource("http://example.org/resource1")
            .addProperty(RDF.type, DCAT.Dataset)
            .addProperty(DCTerms.title, "Test Dataset")

        val model2 = ModelFactory.createDefaultModel()
        model2
            .createResource("http://example.org/resource1")
            .addProperty(DCTerms.title, "Test Dataset")
            .addProperty(RDF.type, DCAT.Dataset)

        val checksum1 = computeChecksum(model1)
        val checksum2 = computeChecksum(model2)

        assertEquals(checksum1, checksum2, "Checksums should be equal for same triples in different order")
        assertEquals(64, checksum1.length, "SHA-256 checksum should be 64 characters")
    }

    @Test
    fun `test computeChecksum produces different results for different models`() {
        val model1 = ModelFactory.createDefaultModel()
        model1
            .createResource("http://example.org/resource1")
            .addProperty(RDF.type, DCAT.Dataset)

        val model2 = ModelFactory.createDefaultModel()
        model2
            .createResource("http://example.org/resource2")
            .addProperty(RDF.type, DCAT.Dataset)

        val checksum1 = computeChecksum(model1)
        val checksum2 = computeChecksum(model2)

        assertNotEquals(checksum1, checksum2, "Checksums should be different for different models")
    }

    @Test
    fun `test parseRDF with valid turtle`() {
        val turtle =
            """
            @prefix dcat: <http://www.w3.org/ns/dcat#> .
            @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
            
            <http://example.org/dataset> a dcat:Dataset .
            """.trimIndent()

        val model = parseRDF(turtle, Lang.TURTLE)
        assertNotNull(model)
        assertFalse(model.isEmpty)
    }

    @Test
    fun `test safeParseRDF with invalid RDF returns empty model`() {
        val invalidRDF = "This is not valid RDF"
        val model = safeParseRDF(invalidRDF, Lang.TURTLE)
        assertNotNull(model)
        assertTrue(model.isEmpty)
    }

    @Test
    fun `test createRDFResponse`() {
        val model = ModelFactory.createDefaultModel()
        model
            .createResource("http://example.org/resource")
            .addProperty(RDF.type, DCAT.Dataset)

        val turtle = model.createRDFResponse(Lang.TURTLE)
        assertNotNull(turtle)
        assertTrue(turtle.contains("http://example.org/resource"))
    }

    @Test
    fun `test createIdFromString produces consistent UUID`() {
        val id1 = createIdFromString("test-string")
        val id2 = createIdFromString("test-string")

        assertEquals(id1, id2)
        assertTrue(id1.matches(Regex("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")))
    }

    @Test
    fun `test containsTriple returns true for existing triple`() {
        val model = ModelFactory.createDefaultModel()
        val resource = model.createResource("http://example.org/resource")
        resource.addProperty(RDF.type, DCAT.Dataset)

        assertTrue(model.containsTriple("<http://example.org/resource>", "a", "<${DCAT.Dataset.uri}>"))
    }

    @Test
    fun `test containsTriple returns false for non-existing triple`() {
        val model = ModelFactory.createDefaultModel()
        assertFalse(model.containsTriple("<http://example.org/resource>", "a", "<http://example.org/Type>"))
    }

    @Test
    fun `test safeAddProperty with null value`() {
        val model = ModelFactory.createDefaultModel()
        val resource = model.createResource("http://example.org/resource")
        val property = DCTerms.title

        val result = resource.safeAddProperty(property, null)
        assertEquals(resource, result)
        assertFalse(resource.hasProperty(property))
    }

    @Test
    fun `test safeAddProperty with empty value`() {
        val model = ModelFactory.createDefaultModel()
        val resource = model.createResource("http://example.org/resource")
        val property = DCTerms.title

        val result = resource.safeAddProperty(property, "")
        assertEquals(resource, result)
        assertFalse(resource.hasProperty(property))
    }

    @Test
    fun `test safeAddProperty with valid value`() {
        val model = ModelFactory.createDefaultModel()
        val resource = model.createResource("http://example.org/resource")
        val property = DCTerms.title

        val result = resource.safeAddProperty(property, "http://example.org/title")
        assertEquals(resource, result)
        assertTrue(resource.hasProperty(property))
    }

    @Test
    fun `test addMetaPrefixes`() {
        val model = ModelFactory.createDefaultModel()
        val result = model.addMetaPrefixes()

        assertEquals(model, result)
        assertNotNull(model.getNsPrefixURI("dct"))
        assertNotNull(model.getNsPrefixURI("dcat"))
        assertNotNull(model.getNsPrefixURI("foaf"))
        assertNotNull(model.getNsPrefixURI("xsd"))
    }

    @Test
    fun `test createCatalogRecordModel with parent produces CatalogRecord with isPartOf`() {
        val issued = Instant.parse("2024-01-15T10:00:00Z")
        val modified = Instant.parse("2024-01-16T12:00:00Z")
        val resourceUri = "http://example.org/datasets/ds1"
        val fdkId = "fdk-ds-1"
        val parentFdkUri = "https://datasets.fellesdatakatalog.digdir.no/catalogs/fdk-cat-1"
        val fdkUriBase = "https://datasets.fellesdatakatalog.digdir.no/datasets"

        val model =
            createCatalogRecordModel(
                resourceUri = resourceUri,
                fdkId = fdkId,
                parentFdkUri = parentFdkUri,
                issued = issued,
                modified = modified,
                fdkUriBase = fdkUriBase,
            )

        assertFalse(model.isEmpty)
        val recordUri = "$fdkUriBase/$fdkId"
        assertTrue(model.containsTriple("<$recordUri>", "a", "<${DCAT.CatalogRecord.uri}>"))
        assertTrue(model.containsTriple("<$recordUri>", "<${DCTerms.identifier.uri}>", "\"$fdkId\""))
        assertTrue(model.containsTriple("<$recordUri>", "<${FOAF.primaryTopic.uri}>", "<$resourceUri>"))
        assertTrue(model.containsTriple("<$recordUri>", "<${DCTerms.isPartOf.uri}>", "<$parentFdkUri>"))
    }

    @Test
    fun `test createCatalogRecordModel without parent omits isPartOf`() {
        val issued = Instant.parse("2024-01-15T10:00:00Z")
        val modified = Instant.parse("2024-01-16T12:00:00Z")
        val catalogUri = "http://example.org/catalogs/cat1"
        val catalogFdkId = "fdk-cat-1"
        val catalogUriBase = "https://datasets.fellesdatakatalog.digdir.no/catalogs"

        val model =
            createCatalogRecordModel(
                resourceUri = catalogUri,
                fdkId = catalogFdkId,
                parentFdkUri = null,
                issued = issued,
                modified = modified,
                fdkUriBase = catalogUriBase,
            )

        assertFalse(model.isEmpty)
        val recordUri = "$catalogUriBase/$catalogFdkId"
        assertTrue(model.containsTriple("<$recordUri>", "a", "<${DCAT.CatalogRecord.uri}>"))
        assertTrue(model.containsTriple("<$recordUri>", "<${DCTerms.identifier.uri}>", "\"$catalogFdkId\""))
        assertTrue(model.containsTriple("<$recordUri>", "<${FOAF.primaryTopic.uri}>", "<$catalogUri>"))
        assertFalse(model.containsTriple("<$recordUri>", "<${DCTerms.isPartOf.uri}>", "?o"))
    }
}
