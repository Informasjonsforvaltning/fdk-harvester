package no.fdk.harvester.rdf

import no.fdk.harvester.Application
import org.apache.jena.query.QueryExecutionFactory
import org.apache.jena.query.QueryFactory
import org.apache.jena.rdf.model.Model
import org.apache.jena.rdf.model.ModelFactory
import org.apache.jena.rdf.model.Property
import org.apache.jena.rdf.model.Resource
import org.apache.jena.riot.Lang
import org.apache.jena.sparql.vocabulary.FOAF
import org.apache.jena.vocabulary.DCAT
import org.apache.jena.vocabulary.DCTerms
import org.apache.jena.vocabulary.RDF
import org.slf4j.LoggerFactory
import java.io.ByteArrayOutputStream
import java.io.StringReader
import java.security.MessageDigest
import java.time.Instant
import java.util.Calendar
import java.util.Locale
import java.util.UUID

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

fun safeParseRDF(
    rdf: String,
    lang: Lang,
): Model =
    try {
        parseRDF(rdf, lang)
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
    UUID
        .nameUUIDFromBytes(idBase.toByteArray())
        .toString()

fun Model.containsTriple(
    subj: String,
    pred: String,
    obj: String,
): Boolean {
    val askQuery = "ASK { $subj $pred $obj }"

    return try {
        val query = QueryFactory.create(askQuery)
        QueryExecutionFactory.create(query, this).execAsk()
    } catch (ex: Exception) {
        false
    }
}

fun Resource.safeAddProperty(
    property: Property,
    value: String?,
): Resource =
    if (value.isNullOrEmpty()) {
        this
    } else {
        addProperty(property, model.createResource(value))
    }

fun parseRDF(
    responseBody: String,
    rdfLanguage: Lang,
): Model {
    val responseModel = ModelFactory.createDefaultModel()
    responseModel.read(StringReader(responseBody), BACKUP_BASE_URI, rdfLanguage.name)

    // test that the model is valid as RDF/XML, will throw exception if not
    responseModel.createRDFResponse(Lang.RDFXML)

    return responseModel
}

fun Model.addMetaPrefixes(): Model {
    setNsPrefix("dct", org.apache.jena.vocabulary.DCTerms.NS)
    setNsPrefix("dcat", org.apache.jena.vocabulary.DCAT.NS)
    setNsPrefix(
        "foaf",
        org.apache.jena.sparql.vocabulary.FOAF
            .getURI(),
    )
    setNsPrefix("xsd", org.apache.jena.vocabulary.XSD.NS)

    return this
}

/**
 * Converts an [Instant] to a [Calendar] for use in Jena typed literals (e.g. xsd:dateTime).
 */
fun calendarFromInstant(instant: Instant): Calendar = Calendar.getInstance().apply { timeInMillis = instant.toEpochMilli() }

/**
 * Builds an RDF model containing a single FDK [dcat:CatalogRecord].
 *
 * @param resourceUri Original resource URI (foaf:primaryTopic).
 * @param fdkId FDK identifier for the resource.
 * @param parentFdkUri Parent catalog or collection FDK URI for dct:isPartOf; null omits the triple.
 * @param issued Issued timestamp for the record.
 * @param modified Modified timestamp for the record.
 * @param fdkUriBase Base URL for FDK resource URIs (record URI is `fdkUriBase/fdkId`).
 * @param missingParentLogMessage When [parentFdkUri] is null, logged at error level if non-null.
 */
fun createCatalogRecordModel(
    resourceUri: String,
    fdkId: String,
    parentFdkUri: String?,
    issued: Instant,
    modified: Instant,
    fdkUriBase: String,
    missingParentLogMessage: String? = null,
): Model {
    val metaModel = ModelFactory.createDefaultModel()
    metaModel.addMetaPrefixes()
    val fdkRecordUri = "$fdkUriBase/$fdkId"
    val metaResource =
        metaModel
            .createResource(fdkRecordUri)
            .addProperty(RDF.type, DCAT.CatalogRecord)
            .addProperty(DCTerms.identifier, metaModel.createLiteral(fdkId))
            .addProperty(FOAF.primaryTopic, metaModel.createResource(resourceUri))
            .addProperty(DCTerms.issued, metaModel.createTypedLiteral(calendarFromInstant(issued)))
            .addProperty(DCTerms.modified, metaModel.createTypedLiteral(calendarFromInstant(modified)))

    when (parentFdkUri) {
        null -> missingParentLogMessage?.let { logger.error(it) }
        else -> metaResource.addProperty(DCTerms.isPartOf, metaModel.createResource(parentFdkUri))
    }

    return metaModel
}

/**
 * Computes a SHA-256 checksum for an RDF model.
 * The checksum is order-independent by sorting triples before hashing.
 */
fun computeChecksum(model: Model): String {
    val triples =
        model
            .listStatements()
            .toList()
            .sortedWith(
                compareBy(
                    { it.subject.toString() },
                    { it.predicate.toString() },
                    { it.`object`.toString() },
                ),
            )

    val digest = MessageDigest.getInstance("SHA-256")
    triples.forEach { statement ->
        val tripleString = "${statement.subject} ${statement.predicate} ${statement.`object`}\n"
        digest.update(tripleString.toByteArray(Charsets.UTF_8))
    }

    return digest.digest().joinToString("") { "%02x".format(it) }
}
