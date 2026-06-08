package no.fdk.harvester.harvester

import no.fdk.harvester.Application
import no.fdk.harvester.model.HarvestSourceEntity
import no.fdk.harvester.model.Organization
import no.fdk.harvester.model.ResourceEntity
import no.fdk.harvester.model.ResourceType
import no.fdk.harvester.rdf.createIdFromString
import no.fdk.harvester.rdf.createRDFResponse
import org.apache.jena.rdf.model.Model
import org.apache.jena.rdf.model.Resource
import org.apache.jena.rdf.model.ResourceFactory
import org.apache.jena.rdf.model.ResourceRequiredException
import org.apache.jena.rdf.model.Statement
import org.apache.jena.riot.Lang
import org.apache.jena.util.ResourceUtils
import org.apache.jena.vocabulary.DCTerms
import org.apache.jena.vocabulary.RDFS
import org.slf4j.LoggerFactory
import java.time.ZoneId
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util.Calendar

private val LOGGER = LoggerFactory.getLogger(Application::class.java)
private const val DATE_FORMAT: String = "yyyy-MM-dd HH:mm:ss Z"

internal fun Model.recursiveBlankNodeSkolem(baseURI: String): Model {
    val anonSubjects = listSubjects().toList().filter { it.isAnon }
    return if (anonSubjects.isEmpty()) {
        this
    } else {
        anonSubjects
            .filter { it.doesNotContainAnon() }
            .forEach {
                ResourceUtils.renameResource(it, "$baseURI/.well-known/skolem/${it.createSkolemID()}")
            }
        this.recursiveBlankNodeSkolem(baseURI)
    }
}

private fun Resource.doesNotContainAnon(): Boolean =
    listProperties()
        .toList()
        .filter { it.isResourceProperty() }
        .map { it.resource }
        .filter { it.listProperties().toList().size > 0 }
        .none { it.isAnon }

private fun Resource.createSkolemID(): String =
    createIdFromString(
        listProperties()
            .toModel()
            .createRDFResponse(Lang.N3)
            .replace("\\s".toRegex(), "")
            .toCharArray()
            .sorted()
            .toString(),
    )

/** Drops blank-node resources (only URI resources can be harvested), logging a warning for each. */
internal fun List<Resource>.excludeBlankNodes(sourceURL: String): List<Resource> =
    filter {
        if (it.isURIResource) {
            true
        } else {
            LOGGER.warn("Blank node resource filtered when harvesting $sourceURL")
            false
        }
    }

internal fun Resource.addPublisherForGeneratedCatalog(publisherURI: String?): Resource {
    if (publisherURI != null) {
        addProperty(DCTerms.publisher, ResourceFactory.createResource(publisherURI))
    }
    return this
}

internal fun Resource.addLabelForGeneratedCatalog(
    organization: Organization?,
    nbnnSuffix: String,
    enSuffix: String,
): Resource {
    val nb: String? = organization?.prefLabel?.nb ?: organization?.name
    if (!nb.isNullOrBlank()) addProperty(RDFS.label, model.createLiteral("$nb - $nbnnSuffix", "nb"))

    val nn: String? = organization?.prefLabel?.nn ?: organization?.name
    if (!nn.isNullOrBlank()) addProperty(RDFS.label, model.createLiteral("$nn - $nbnnSuffix", "nn"))

    val en: String? = organization?.prefLabel?.en ?: organization?.name
    if (!en.isNullOrBlank()) addProperty(RDFS.label, model.createLiteral("$en - $enSuffix", "en"))

    return this
}

fun Statement.isResourceProperty(): Boolean =
    try {
        resource.isResource
    } catch (ex: ResourceRequiredException) {
        false
    }

fun formatNowWithOsloTimeZone(): String =
    ZonedDateTime
        .now(ZoneId.of("Europe/Oslo"))
        .format(DateTimeFormatter.ofPattern(DATE_FORMAT))

fun Calendar.formatWithOsloTimeZone(): String =
    ZonedDateTime
        .from(toInstant().atZone(ZoneId.of("Europe/Oslo")))
        .format(DateTimeFormatter.ofPattern(DATE_FORMAT))

fun createResourceEntity(
    uri: String,
    type: ResourceType,
    checksum: String,
    harvestDate: Calendar,
    harvestSource: HarvestSourceEntity,
    dbMeta: ResourceEntity?,
): ResourceEntity =
    ResourceEntity(
        uri = uri,
        type = type,
        fdkId = dbMeta?.fdkId ?: createIdFromString(uri),
        removed = false,
        issued = dbMeta?.issued ?: harvestDate.toInstant(),
        modified = harvestDate.toInstant(),
        checksum = checksum,
        harvestSource = harvestSource,
    )

fun checksumHasChanged(
    dbMeta: ResourceEntity?,
    checksum: String,
): Boolean = dbMeta == null || checksum != dbMeta.checksum

class HarvestException(
    url: String,
) : Exception("Harvest failed for $url")

class HarvestSourceConflictException(
    message: String,
) : Exception(message)
