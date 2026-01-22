package no.fdk.fdk_harvester.rdf

import org.apache.jena.rdf.model.ModelFactory
import org.apache.jena.riot.Lang
import org.apache.jena.sparql.vocabulary.FOAF
import org.apache.jena.vocabulary.DCAT
import org.apache.jena.vocabulary.DCTerms
import org.apache.jena.vocabulary.RDF
import org.junit.jupiter.api.Assertions.*
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
        model1.createResource("http://example.org/resource1")
            .addProperty(RDF.type, DCAT.Dataset)
            .addProperty(DCTerms.title, "Test Dataset")

        val model2 = ModelFactory.createDefaultModel()
        model2.createResource("http://example.org/resource1")
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
        model1.createResource("http://example.org/resource1")
            .addProperty(RDF.type, DCAT.Dataset)

        val model2 = ModelFactory.createDefaultModel()
        model2.createResource("http://example.org/resource2")
            .addProperty(RDF.type, DCAT.Dataset)

        val checksum1 = computeChecksum(model1)
        val checksum2 = computeChecksum(model2)
        
        assertNotEquals(checksum1, checksum2, "Checksums should be different for different models")
    }

    @Test
    fun `test parseRDFResponse with valid turtle`() {
        val turtle = """
            @prefix dcat: <http://www.w3.org/ns/dcat#> .
            @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
            
            <http://example.org/dataset> a dcat:Dataset .
        """.trimIndent()

        val model = parseRDFResponse(turtle, Lang.TURTLE)
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
        model.createResource("http://example.org/resource")
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
    fun `test createDatasetCatalogRecordModel produces CatalogRecord with required properties`() {
        val issued = Instant.parse("2024-01-15T10:00:00Z")
        val modified = Instant.parse("2024-01-16T12:00:00Z")
        val datasetUri = "http://example.org/datasets/ds1"
        val datasetFdkId = "fdk-ds-1"
        val catalogFdkUri = "https://datasets.fellesdatakatalog.digdir.no/catalogs/fdk-cat-1"
        val datasetUriBase = "https://datasets.fellesdatakatalog.digdir.no/datasets"

        val model = createDatasetCatalogRecordModel(
            datasetUri = datasetUri,
            datasetFdkId = datasetFdkId,
            catalogFdkUri = catalogFdkUri,
            issued = issued,
            modified = modified,
            datasetUriBase = datasetUriBase
        )

        assertFalse(model.isEmpty)
        val recordUri = "$datasetUriBase/$datasetFdkId"
        assertTrue(model.containsTriple("<$recordUri>", "a", "<${DCAT.CatalogRecord.uri}>"))
        assertTrue(model.containsTriple("<$recordUri>", "<${DCTerms.identifier.uri}>", "\"$datasetFdkId\""))
        assertTrue(model.containsTriple("<$recordUri>", "<${DCTerms.isPartOf.uri}>", "<$catalogFdkUri>"))
    }

    @Test
    fun `test createDataServiceCatalogRecordModel produces CatalogRecord with required properties`() {
        val issued = Instant.parse("2024-01-15T10:00:00Z")
        val modified = Instant.parse("2024-01-16T12:00:00Z")
        val dataserviceUri = "http://example.org/dataservices/ds1"
        val dataserviceFdkId = "fdk-ds-1"
        val catalogFdkUri = "https://dataservices.fellesdatakatalog.digdir.no/catalogs/fdk-cat-1"
        val dataserviceUriBase = "https://dataservices.fellesdatakatalog.digdir.no/dataservices"

        val model = createDataServiceCatalogRecordModel(
            dataserviceUri = dataserviceUri,
            dataserviceFdkId = dataserviceFdkId,
            catalogFdkUri = catalogFdkUri,
            issued = issued,
            modified = modified,
            dataserviceUriBase = dataserviceUriBase
        )

        assertFalse(model.isEmpty)
        val recordUri = "$dataserviceUriBase/$dataserviceFdkId"
        assertTrue(model.containsTriple("<$recordUri>", "a", "<${DCAT.CatalogRecord.uri}>"))
        assertTrue(model.containsTriple("<$recordUri>", "<${DCTerms.identifier.uri}>", "\"$dataserviceFdkId\""))
        assertTrue(model.containsTriple("<$recordUri>", "<${DCTerms.isPartOf.uri}>", "<$catalogFdkUri>"))
    }

    @Test
    fun `test createInformationModelCatalogRecordModel produces CatalogRecord with required properties`() {
        val issued = Instant.parse("2024-01-15T10:00:00Z")
        val modified = Instant.parse("2024-01-16T12:00:00Z")
        val informationModelUri = "http://example.org/informationmodels/im1"
        val informationModelFdkId = "fdk-im-1"
        val catalogFdkUri = "https://informationmodels.fellesdatakatalog.digdir.no/catalogs/fdk-cat-1"
        val informationModelUriBase = "https://informationmodels.fellesdatakatalog.digdir.no/informationmodels"

        val model = createInformationModelCatalogRecordModel(
            informationModelUri = informationModelUri,
            informationModelFdkId = informationModelFdkId,
            catalogFdkUri = catalogFdkUri,
            issued = issued,
            modified = modified,
            informationModelUriBase = informationModelUriBase
        )

        assertFalse(model.isEmpty)
        val recordUri = "$informationModelUriBase/$informationModelFdkId"
        assertTrue(model.containsTriple("<$recordUri>", "a", "<${DCAT.CatalogRecord.uri}>"))
        assertTrue(model.containsTriple("<$recordUri>", "<${DCTerms.identifier.uri}>", "\"$informationModelFdkId\""))
        assertTrue(model.containsTriple("<$recordUri>", "<${DCTerms.isPartOf.uri}>", "<$catalogFdkUri>"))
    }

    @Test
    fun `test createEventCatalogRecordModel produces CatalogRecord with required properties`() {
        val issued = Instant.parse("2024-01-15T10:00:00Z")
        val modified = Instant.parse("2024-01-16T12:00:00Z")
        val eventUri = "http://example.org/events/ev1"
        val eventFdkId = "fdk-ev-1"
        val catalogFdkUri = "https://events.fellesdatakatalog.digdir.no/catalogs/fdk-cat-1"
        val eventUriBase = "https://events.fellesdatakatalog.digdir.no/events"

        val model = createEventCatalogRecordModel(
            eventUri = eventUri,
            eventFdkId = eventFdkId,
            catalogFdkUri = catalogFdkUri,
            issued = issued,
            modified = modified,
            eventUriBase = eventUriBase
        )

        assertFalse(model.isEmpty)
        val recordUri = "$eventUriBase/$eventFdkId"
        assertTrue(model.containsTriple("<$recordUri>", "a", "<${DCAT.CatalogRecord.uri}>"))
        assertTrue(model.containsTriple("<$recordUri>", "<${DCTerms.isPartOf.uri}>", "<$catalogFdkUri>"))
    }

    @Test
    fun `test createServiceCatalogRecordModel produces CatalogRecord with required properties`() {
        val issued = Instant.parse("2024-01-15T10:00:00Z")
        val modified = Instant.parse("2024-01-16T12:00:00Z")
        val serviceUri = "http://example.org/services/svc1"
        val serviceFdkId = "fdk-svc-1"
        val catalogFdkUri = "https://services.fellesdatakatalog.digdir.no/catalogs/fdk-cat-1"
        val serviceUriBase = "https://services.fellesdatakatalog.digdir.no/services"

        val model = createServiceCatalogRecordModel(
            serviceUri = serviceUri,
            serviceFdkId = serviceFdkId,
            catalogFdkUri = catalogFdkUri,
            issued = issued,
            modified = modified,
            serviceUriBase = serviceUriBase
        )

        assertFalse(model.isEmpty)
        val recordUri = "$serviceUriBase/$serviceFdkId"
        assertTrue(model.containsTriple("<$recordUri>", "a", "<${DCAT.CatalogRecord.uri}>"))
        assertTrue(model.containsTriple("<$recordUri>", "<${DCTerms.isPartOf.uri}>", "<$catalogFdkUri>"))
    }

    @Test
    fun `test createConceptCatalogRecordModel produces CatalogRecord with isPartOf collection`() {
        val issued = Instant.parse("2024-01-15T10:00:00Z")
        val modified = Instant.parse("2024-01-16T12:00:00Z")
        val conceptUri = "http://example.org/concepts/c1"
        val conceptFdkId = "fdk-concept-1"
        val collectionFdkUri = "https://concepts.fellesdatakatalog.digdir.no/collections/fdk-coll-1"
        val conceptUriBase = "https://concepts.fellesdatakatalog.digdir.no/concepts"

        val model = createConceptCatalogRecordModel(
            conceptUri = conceptUri,
            conceptFdkId = conceptFdkId,
            collectionFdkUri = collectionFdkUri,
            issued = issued,
            modified = modified,
            conceptUriBase = conceptUriBase
        )

        assertFalse(model.isEmpty)
        val recordUri = "$conceptUriBase/$conceptFdkId"
        assertTrue(model.containsTriple("<$recordUri>", "a", "<${DCAT.CatalogRecord.uri}>"))
        assertTrue(model.containsTriple("<$recordUri>", "<${DCTerms.isPartOf.uri}>", "<$collectionFdkUri>"))
    }

    @Test
    fun `test createCatalogCatalogRecordModel produces CatalogRecord with required properties`() {
        val issued = Instant.parse("2024-01-15T10:00:00Z")
        val modified = Instant.parse("2024-01-16T12:00:00Z")
        val catalogUri = "http://example.org/catalogs/cat1"
        val catalogFdkId = "fdk-cat-1"
        val catalogUriBase = "https://datasets.fellesdatakatalog.digdir.no/catalogs"

        val model = createCatalogCatalogRecordModel(
            catalogUri = catalogUri,
            catalogFdkId = catalogFdkId,
            issued = issued,
            modified = modified,
            catalogUriBase = catalogUriBase
        )

        assertFalse(model.isEmpty)
        val recordUri = "$catalogUriBase/$catalogFdkId"
        assertTrue(model.containsTriple("<$recordUri>", "a", "<${DCAT.CatalogRecord.uri}>"))
        assertTrue(model.containsTriple("<$recordUri>", "<${DCTerms.identifier.uri}>", "\"$catalogFdkId\""))
        assertTrue(model.containsTriple("<$recordUri>", "<${FOAF.primaryTopic.uri}>", "<$catalogUri>"))
    }
}

