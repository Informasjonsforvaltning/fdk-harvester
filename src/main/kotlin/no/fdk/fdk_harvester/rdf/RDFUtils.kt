package no.fdk.fdk_harvester.rdf

import no.fdk.fdk_harvester.Application

/**
 * RDF parsing and serialization utilities: map Accept header to Jena [Lang], parse RDF strings
 * into [Model], safe parse (returns empty model on failure), checksum, and SPARQL ASK helpers.
 */
import org.apache.jena.query.QueryExecutionFactory
import org.apache.jena.query.QueryFactory
import org.apache.jena.rdf.model.Model
import org.apache.jena.rdf.model.ModelFactory
import org.apache.jena.rdf.model.Property
import org.apache.jena.rdf.model.Resource
import org.apache.jena.riot.Lang
import org.slf4j.LoggerFactory
import org.apache.jena.sparql.vocabulary.FOAF
import org.apache.jena.vocabulary.DCAT
import org.apache.jena.vocabulary.DCTerms
import org.apache.jena.vocabulary.RDF
import java.io.ByteArrayOutputStream
import java.io.StringReader
import java.security.MessageDigest
import java.time.Instant
import java.util.Locale
import java.util.*

private val logger = LoggerFactory.getLogger(Application::class.java)
const val BACKUP_BASE_URI = "http://example.com/"

fun jenaTypeFromAcceptHeader(accept: String?): Lang? {
    val a = accept?.lowercase(Locale.ROOT) ?: return null

    return when {
        a.contains("text/turtle") || a.contains("application/turtle") -> Lang.TURTLE
        a.contains("text/n3") -> Lang.N3
        a.contains("application/trig") -> Lang.TRIG
        a.contains("application/rdf+xml") -> Lang.RDFXML
        a.contains("application/rdf+json") -> Lang.RDFJSON
        a.contains("application/ld+json") -> Lang.JSONLD
        a.contains("application/n-triples") || a.contains("application/ntriples") -> Lang.NTRIPLES
        a.contains("application/n-quads") || a.contains("application/nquads") -> Lang.NQUADS
        a.contains("application/trix") || a.contains("application/trix+xml") -> Lang.TRIX
        a.contains("*/*") -> null
        else -> Lang.RDFNULL
    }
}

fun parseRDFResponse(responseBody: String, rdfLanguage: Lang): Model {
    val responseModel = ModelFactory.createDefaultModel()
    responseModel.read(StringReader(responseBody), BACKUP_BASE_URI, rdfLanguage.name)

    // test that the model is valid as RDF/XML, will throw exception if not
    responseModel.createRDFResponse(Lang.RDFXML)

    return responseModel
}

fun safeParseRDF(rdf: String, lang: Lang): Model =
    try {
        parseRDFResponse(rdf, lang)
    } catch (ex: Exception) {
        logger.warn("parse failure", ex)
        ModelFactory.createDefaultModel()
    }

fun Model.createRDFResponse(responseType: Lang): String =
    ByteArrayOutputStream().use { out ->
        write(out, responseType.name)
        out.flush()
        out.toString("UTF-8")
    }

fun createIdFromString(idBase: String): String =
    UUID.nameUUIDFromBytes(idBase.toByteArray())
        .toString()

fun Model.containsTriple(subj: String, pred: String, obj: String): Boolean {
    val askQuery = "ASK { $subj $pred $obj }"

    return try {
        val query = QueryFactory.create(askQuery)
        QueryExecutionFactory.create(query, this).execAsk()
    } catch (ex: Exception) { false }
}

fun Resource.safeAddProperty(property: Property, value: String?): Resource =
    if (value.isNullOrEmpty()) this
    else addProperty(property, model.createResource(value))

fun parseRDF(responseBody: String, rdfLanguage: Lang): Model {
    val responseModel = ModelFactory.createDefaultModel()
    responseModel.read(StringReader(responseBody), BACKUP_BASE_URI, rdfLanguage.name)

    // test that the model is valid as RDF/XML, will throw exception if not
    responseModel.createRDFResponse(Lang.RDFXML)

    return responseModel
}

fun Model.addMetaPrefixes(): Model {
    setNsPrefix("dct", org.apache.jena.vocabulary.DCTerms.NS)
    setNsPrefix("dcat", org.apache.jena.vocabulary.DCAT.NS)
    setNsPrefix("foaf", org.apache.jena.sparql.vocabulary.FOAF.getURI())
    setNsPrefix("xsd", org.apache.jena.vocabulary.XSD.NS)

    return this
}

/**
 * Converts an [Instant] to a [Calendar] for use in Jena typed literals (e.g. xsd:dateTime).
 */
fun calendarFromInstant(instant: Instant): Calendar =
    Calendar.getInstance().apply { timeInMillis = instant.toEpochMilli() }

/**
 * Builds an RDF model containing a single FDK [dcat:CatalogRecord] for a dataset.
 * Used to add catalog record triples to harvested dataset graphs (like the legacy dataset harvester).
 *
 * @param datasetUri The original dataset resource URI (foaf:primaryTopic).
 * @param datasetFdkId FDK identifier for the dataset.
 * @param catalogFdkUri Full FDK catalog URI (catalogUri + "/" + catalogFdkId), used for dct:isPartOf.
 * @param issued Issued timestamp for the record.
 * @param modified Modified timestamp for the record.
 * @param datasetUriBase Base URL for FDK dataset URIs (e.g. https://datasets.fellesdatakatalog.digdir.no/datasets).
 */
fun createDatasetCatalogRecordModel(
    datasetUri: String,
    datasetFdkId: String,
    catalogFdkUri: String,
    issued: Instant,
    modified: Instant,
    datasetUriBase: String
): Model {
    val metaModel = ModelFactory.createDefaultModel()
    metaModel.addMetaPrefixes()
    val fdkRecordUri = "$datasetUriBase/$datasetFdkId"
    metaModel.createResource(fdkRecordUri)
        .addProperty(RDF.type, DCAT.CatalogRecord)
        .addProperty(DCTerms.identifier, metaModel.createLiteral(datasetFdkId))
        .addProperty(FOAF.primaryTopic, metaModel.createResource(datasetUri))
        .addProperty(DCTerms.isPartOf, metaModel.createResource(catalogFdkUri))
        .addProperty(DCTerms.issued, metaModel.createTypedLiteral(calendarFromInstant(issued)))
        .addProperty(DCTerms.modified, metaModel.createTypedLiteral(calendarFromInstant(modified)))
    return metaModel
}

/**
 * Builds an RDF model containing a single FDK [dcat:CatalogRecord] for a data service.
 * Used to add catalog record triples to harvested dataservice graphs (like the dataset harvester).
 *
 * @param dataserviceUri The original dataservice resource URI (foaf:primaryTopic).
 * @param dataserviceFdkId FDK identifier for the dataservice.
 * @param catalogFdkUri Full FDK catalog URI (catalogUri + "/" + catalogFdkId), used for dct:isPartOf.
 * @param issued Issued timestamp for the record.
 * @param modified Modified timestamp for the record.
 * @param dataserviceUriBase Base URL for FDK dataservice URIs (e.g. https://dataservices.fellesdatakatalog.digdir.no/dataservices).
 */
fun createDataServiceCatalogRecordModel(
    dataserviceUri: String,
    dataserviceFdkId: String,
    catalogFdkUri: String,
    issued: Instant,
    modified: Instant,
    dataserviceUriBase: String
): Model {
    val metaModel = ModelFactory.createDefaultModel()
    metaModel.addMetaPrefixes()
    val fdkRecordUri = "$dataserviceUriBase/$dataserviceFdkId"
    metaModel.createResource(fdkRecordUri)
        .addProperty(RDF.type, DCAT.CatalogRecord)
        .addProperty(DCTerms.identifier, metaModel.createLiteral(dataserviceFdkId))
        .addProperty(FOAF.primaryTopic, metaModel.createResource(dataserviceUri))
        .addProperty(DCTerms.isPartOf, metaModel.createResource(catalogFdkUri))
        .addProperty(DCTerms.issued, metaModel.createTypedLiteral(calendarFromInstant(issued)))
        .addProperty(DCTerms.modified, metaModel.createTypedLiteral(calendarFromInstant(modified)))
    return metaModel
}

/**
 * Builds an RDF model containing a single FDK [dcat:CatalogRecord] for an information model.
 * Used to add catalog record triples to harvested information model graphs.
 *
 * @param informationModelUri The original information model resource URI (foaf:primaryTopic).
 * @param informationModelFdkId FDK identifier for the information model.
 * @param catalogFdkUri Full FDK catalog URI (catalogUri + "/" + catalogFdkId), used for dct:isPartOf.
 * @param issued Issued timestamp for the record.
 * @param modified Modified timestamp for the record.
 * @param informationModelUriBase Base URL for FDK information model URIs (e.g. https://informationmodels.fellesdatakatalog.digdir.no/informationmodels).
 */
fun createInformationModelCatalogRecordModel(
    informationModelUri: String,
    informationModelFdkId: String,
    catalogFdkUri: String,
    issued: Instant,
    modified: Instant,
    informationModelUriBase: String
): Model {
    val metaModel = ModelFactory.createDefaultModel()
    metaModel.addMetaPrefixes()
    val fdkRecordUri = "$informationModelUriBase/$informationModelFdkId"
    metaModel.createResource(fdkRecordUri)
        .addProperty(RDF.type, DCAT.CatalogRecord)
        .addProperty(DCTerms.identifier, metaModel.createLiteral(informationModelFdkId))
        .addProperty(FOAF.primaryTopic, metaModel.createResource(informationModelUri))
        .addProperty(DCTerms.isPartOf, metaModel.createResource(catalogFdkUri))
        .addProperty(DCTerms.issued, metaModel.createTypedLiteral(calendarFromInstant(issued)))
        .addProperty(DCTerms.modified, metaModel.createTypedLiteral(calendarFromInstant(modified)))
    return metaModel
}

/**
 * Builds an RDF model containing a single FDK [dcat:CatalogRecord] for an event.
 * Used to add catalog record triples to harvested event graphs.
 */
fun createEventCatalogRecordModel(
    eventUri: String,
    eventFdkId: String,
    catalogFdkUri: String,
    issued: Instant,
    modified: Instant,
    eventUriBase: String
): Model {
    val metaModel = ModelFactory.createDefaultModel()
    metaModel.addMetaPrefixes()
    val fdkRecordUri = "$eventUriBase/$eventFdkId"
    metaModel.createResource(fdkRecordUri)
        .addProperty(RDF.type, DCAT.CatalogRecord)
        .addProperty(DCTerms.identifier, metaModel.createLiteral(eventFdkId))
        .addProperty(FOAF.primaryTopic, metaModel.createResource(eventUri))
        .addProperty(DCTerms.isPartOf, metaModel.createResource(catalogFdkUri))
        .addProperty(DCTerms.issued, metaModel.createTypedLiteral(calendarFromInstant(issued)))
        .addProperty(DCTerms.modified, metaModel.createTypedLiteral(calendarFromInstant(modified)))
    return metaModel
}

/**
 * Builds an RDF model containing a single FDK [dcat:CatalogRecord] for a service.
 * Used to add catalog record triples to harvested service graphs.
 */
fun createServiceCatalogRecordModel(
    serviceUri: String,
    serviceFdkId: String,
    catalogFdkUri: String,
    issued: Instant,
    modified: Instant,
    serviceUriBase: String
): Model {
    val metaModel = ModelFactory.createDefaultModel()
    metaModel.addMetaPrefixes()
    val fdkRecordUri = "$serviceUriBase/$serviceFdkId"
    metaModel.createResource(fdkRecordUri)
        .addProperty(RDF.type, DCAT.CatalogRecord)
        .addProperty(DCTerms.identifier, metaModel.createLiteral(serviceFdkId))
        .addProperty(FOAF.primaryTopic, metaModel.createResource(serviceUri))
        .addProperty(DCTerms.isPartOf, metaModel.createResource(catalogFdkUri))
        .addProperty(DCTerms.issued, metaModel.createTypedLiteral(calendarFromInstant(issued)))
        .addProperty(DCTerms.modified, metaModel.createTypedLiteral(calendarFromInstant(modified)))
    return metaModel
}

/**
 * Builds an RDF model containing a single FDK [dcat:CatalogRecord] for a concept.
 * Concepts use collections (not catalogs); the record's dct:isPartOf points to the collection FDK URI.
 * Used to add catalog record triples to harvested concept graphs.
 */
fun createConceptCatalogRecordModel(
    conceptUri: String,
    conceptFdkId: String,
    collectionFdkUri: String,
    issued: Instant,
    modified: Instant,
    conceptUriBase: String
): Model {
    val metaModel = ModelFactory.createDefaultModel()
    metaModel.addMetaPrefixes()
    val fdkRecordUri = "$conceptUriBase/$conceptFdkId"
    metaModel.createResource(fdkRecordUri)
        .addProperty(RDF.type, DCAT.CatalogRecord)
        .addProperty(DCTerms.identifier, metaModel.createLiteral(conceptFdkId))
        .addProperty(FOAF.primaryTopic, metaModel.createResource(conceptUri))
        .addProperty(DCTerms.isPartOf, metaModel.createResource(collectionFdkUri))
        .addProperty(DCTerms.issued, metaModel.createTypedLiteral(calendarFromInstant(issued)))
        .addProperty(DCTerms.modified, metaModel.createTypedLiteral(calendarFromInstant(modified)))
    return metaModel
}

/**
 * Builds an RDF model containing a single FDK [dcat:CatalogRecord] for a catalog.
 * Used when publishing catalog graphs with records (e.g. for API or events).
 *
 * @param catalogUri The original catalog resource URI (foaf:primaryTopic).
 * @param catalogFdkId FDK identifier for the catalog.
 * @param issued Issued timestamp for the record.
 * @param modified Modified timestamp for the record.
 * @param catalogUriBase Base URL for FDK catalog URIs (e.g. https://datasets.fellesdatakatalog.digdir.no/catalogs).
 */
fun createCatalogCatalogRecordModel(
    catalogUri: String,
    catalogFdkId: String,
    issued: Instant,
    modified: Instant,
    catalogUriBase: String
): Model {
    val metaModel = ModelFactory.createDefaultModel()
    metaModel.addMetaPrefixes()
    val fdkRecordUri = "$catalogUriBase/$catalogFdkId"
    metaModel.createResource(fdkRecordUri)
        .addProperty(RDF.type, DCAT.CatalogRecord)
        .addProperty(DCTerms.identifier, metaModel.createLiteral(catalogFdkId))
        .addProperty(FOAF.primaryTopic, metaModel.createResource(catalogUri))
        .addProperty(DCTerms.issued, metaModel.createTypedLiteral(calendarFromInstant(issued)))
        .addProperty(DCTerms.modified, metaModel.createTypedLiteral(calendarFromInstant(modified)))
    return metaModel
}

/**
 * Computes a SHA-256 checksum for an RDF model.
 * The checksum is order-independent by sorting triples before hashing.
 */
fun computeChecksum(model: Model): String {
    val triples = model.listStatements().toList()
        .sortedWith(compareBy(
            { it.subject.toString() },
            { it.predicate.toString() },
            { it.`object`.toString() }
        ))
    
    val digest = MessageDigest.getInstance("SHA-256")
    triples.forEach { statement ->
        val tripleString = "${statement.subject} ${statement.predicate} ${statement.`object`}\n"
        digest.update(tripleString.toByteArray(Charsets.UTF_8))
    }
    
    return digest.digest().joinToString("") { "%02x".format(it) }
}
