package no.fdk.harvester.harvester

import no.fdk.harvester.Application
import no.fdk.harvester.model.HarvestSourceEntity
import no.fdk.harvester.model.Organization
import no.fdk.harvester.model.ResourceEntity
import no.fdk.harvester.model.ResourceType
import no.fdk.harvester.rdf.containsTriple
import no.fdk.harvester.rdf.createIdFromString
import no.fdk.harvester.rdf.createRDFResponse
import no.fdk.harvester.rdf.safeParseRDF
import org.apache.jena.query.QueryExecutionFactory
import org.apache.jena.query.QueryFactory
import org.apache.jena.rdf.model.Model
import org.apache.jena.rdf.model.ModelFactory
import org.apache.jena.rdf.model.Resource
import org.apache.jena.rdf.model.ResourceFactory
import org.apache.jena.rdf.model.ResourceRequiredException
import org.apache.jena.rdf.model.Statement
import org.apache.jena.riot.Lang
import org.apache.jena.util.ResourceUtils
import org.apache.jena.vocabulary.DCTerms
import org.apache.jena.vocabulary.RDF
import org.apache.jena.vocabulary.RDFS
import org.apache.jena.vocabulary.SKOS
import org.slf4j.LoggerFactory
import java.time.ZoneId
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util.Calendar

private val LOGGER = LoggerFactory.getLogger(Application::class.java)
private const val DATE_FORMAT: String = "yyyy-MM-dd HH:mm:ss Z"

fun CollectionRDFModel.harvestDiff(dboNoRecords: String?): Boolean =
    if (dboNoRecords == null) {
        true
    } else {
        !harvested.isIsomorphicWith(safeParseRDF(dboNoRecords, Lang.TURTLE))
    }

fun ConceptRDFModel.harvestDiff(dboNoRecords: String?): Boolean =
    if (dboNoRecords == null) {
        true
    } else {
        !harvested.isIsomorphicWith(safeParseRDF(dboNoRecords, Lang.TURTLE))
    }

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

fun splitCollectionsFromRDF(
    harvested: Model,
    allConcepts: List<ConceptRDFModel>,
    sourceURL: String,
    organization: Organization?,
): List<CollectionRDFModel> {
    val harvestedCollections =
        harvested
            .listResourcesWithProperty(RDF.type, SKOS.Collection)
            .toList()
            .excludeBlankNodeCollectionsAndConcepts(sourceURL)
            .map { collectionResource ->
                val collectionConcepts: Set<String> =
                    collectionResource
                        .listProperties(SKOS.member)
                        .toList()
                        .map { it.resource }
                        .excludeBlankNodeCollectionsAndConcepts(sourceURL)
                        .map { it.uri }
                        .toSet()

                val collectionModelWithoutConcepts =
                    collectionResource
                        .extractCollectionModel()
                        .recursiveBlankNodeSkolem(collectionResource.uri)

                val collectionModel = ModelFactory.createDefaultModel()
                allConcepts
                    .filter { collectionConcepts.contains(it.resourceURI) }
                    .forEach { collectionModel.add(it.harvested) }

                CollectionRDFModel(
                    resourceURI = collectionResource.uri,
                    harvestedWithoutConcepts = collectionModelWithoutConcepts,
                    harvested = collectionModel.union(collectionModelWithoutConcepts),
                    concepts = collectionConcepts,
                )
            }

    return harvestedCollections.plus(
        generatedCollection(
            allConcepts.filterNot { it.isMemberOfAnyCollection },
            sourceURL,
            organization,
        ),
    )
}

private fun List<Resource>.excludeBlankNodeCollectionsAndConcepts(sourceURL: String): List<Resource> =
    filter {
        if (it.isURIResource) {
            true
        } else {
            LOGGER.error(
                "Failed harvest of collection or concept for $sourceURL, unable to harvest blank node collections and concepts",
                Exception("unable to harvest blank node collections and concepts"),
            )
            false
        }
    }

fun splitConceptsFromRDF(
    harvested: Model,
    sourceURL: String,
): List<ConceptRDFModel> =
    harvested
        .listResourcesWithProperty(RDF.type, SKOS.Concept)
        .toList()
        .excludeBlankNodeCollectionsAndConcepts(sourceURL)
        .map { conceptResource -> conceptResource.extractConcept() }

fun Resource.extractCollectionModel(): Model {
    val collectionModelWithoutConcepts = ModelFactory.createDefaultModel()
    collectionModelWithoutConcepts.setNsPrefixes(model.nsPrefixMap)

    listProperties()
        .toList()
        .forEach { collectionModelWithoutConcepts.addCatalogProperties(it) }

    return collectionModelWithoutConcepts
}

private fun Model.addCatalogProperties(property: Statement): Model =
    when {
        property.predicate != SKOS.member && property.isResourceProperty() -> {
            add(property).recursiveAddNonConceptResource(property.resource)
        }

        property.predicate != SKOS.member -> {
            add(property)
        }

        property.isResourceProperty() && property.resource.isURIResource -> {
            add(property)
        }

        else -> {
            this
        }
    }

fun Resource.extractConcept(): ConceptRDFModel {
    val conceptModel = listProperties().toModel()
    conceptModel.setNsPrefixes(model.nsPrefixMap)

    listProperties()
        .toList()
        .filter { it.isResourceProperty() }
        .forEach { conceptModel.recursiveAddNonConceptResource(it.resource) }

    return ConceptRDFModel(
        resourceURI = uri,
        harvested = conceptModel.recursiveBlankNodeSkolem(uri),
        isMemberOfAnyCollection = isMemberOfAnyCollection(),
    )
}

private fun Model.recursiveAddNonConceptResource(resource: Resource): Model {
    if (resourceShouldBeAdded(resource)) {
        add(resource.listProperties())

        resource
            .listProperties()
            .toList()
            .filter { it.isResourceProperty() }
            .forEach { recursiveAddNonConceptResource(it.resource) }
    }

    return this
}

private fun generatedCollection(
    concepts: List<ConceptRDFModel>,
    sourceURL: String,
    organization: Organization?,
): CollectionRDFModel {
    val conceptURIs = concepts.map { it.resourceURI }.toSet()
    val generatedCollectionURI = "$sourceURL#GeneratedCollection"
    val collectionModelWithoutConcepts = createModelForHarvestSourceCollection(generatedCollectionURI, conceptURIs, organization)

    val collectionModel = ModelFactory.createDefaultModel()
    concepts.forEach { collectionModel.add(it.harvested) }

    return CollectionRDFModel(
        resourceURI = generatedCollectionURI,
        harvestedWithoutConcepts = collectionModelWithoutConcepts,
        harvested = collectionModel.union(collectionModelWithoutConcepts),
        concepts = conceptURIs,
    )
}

private fun createModelForHarvestSourceCollection(
    collectionURI: String,
    concepts: Set<String>,
    organization: Organization?,
): Model {
    val collectionModel = ModelFactory.createDefaultModel()
    collectionModel
        .createResource(collectionURI)
        .addProperty(RDF.type, SKOS.Collection)
        .addPublisherForGeneratedCatalog(organization?.uri)
        .addLabelForGeneratedCatalog(organization, "Begrepssamling", "Concept collection")
        .addMembersForGeneratedCollection(concepts)

    return collectionModel
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

private fun Resource.addMembersForGeneratedCollection(concepts: Set<String>): Resource {
    concepts.forEach { addProperty(SKOS.member, model.createResource(it)) }
    return this
}

fun Statement.isResourceProperty(): Boolean =
    try {
        resource.isResource
    } catch (ex: ResourceRequiredException) {
        false
    }

fun calendarFromTimestamp(timestamp: Long): Calendar {
    val calendar = Calendar.getInstance()
    calendar.timeInMillis = timestamp
    return calendar
}

data class CollectionRDFModel(
    val resourceURI: String,
    val harvested: Model,
    val harvestedWithoutConcepts: Model,
    val concepts: Set<String>,
)

data class ConceptRDFModel(
    val resourceURI: String,
    val harvested: Model,
    val isMemberOfAnyCollection: Boolean,
)

private fun Model.resourceShouldBeAdded(resource: Resource): Boolean {
    val types =
        resource
            .listProperties(RDF.type)
            .toList()
            .map { it.`object` }

    return when {
        types.contains(SKOS.Concept) -> false
        !resource.isURIResource -> true
        containsTriple("<${resource.uri}>", "a", "?o") -> false
        else -> true
    }
}

private fun Resource.isMemberOfAnyCollection(): Boolean {
    val askQuery =
        """ASK {
        ?collection a <${SKOS.Collection.uri}> .
        ?collection <${SKOS.member.uri}> <$uri> .
    }
        """.trimMargin()

    val query = QueryFactory.create(askQuery)
    return QueryExecutionFactory.create(query, model).execAsk()
}

fun List<ConceptRDFModel>.containsConceptsWithoutCollection(): Boolean = firstOrNull { !it.isMemberOfAnyCollection } != null

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
