package no.fdk.fdk_harvester.harvester

import no.fdk.fdk_harvester.Application
import no.fdk.fdk_harvester.model.Organization
import no.fdk.fdk_harvester.rdf.*
import org.apache.jena.query.QueryExecutionFactory
import org.apache.jena.query.QueryFactory
import org.apache.jena.rdf.model.*
import org.apache.jena.riot.Lang
import org.apache.jena.util.ResourceUtils
import org.apache.jena.vocabulary.*
import org.slf4j.LoggerFactory
import java.time.ZoneId
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util.*

private val LOGGER = LoggerFactory.getLogger(Application::class.java)
private const val dateFormat: String = "yyyy-MM-dd HH:mm:ss Z"

fun CollectionRDFModel.harvestDiff(dboNoRecords: String?): Boolean =
    if (dboNoRecords == null) true
    else !harvested.isIsomorphicWith(safeParseRDF(dboNoRecords, Lang.TURTLE))

fun ConceptRDFModel.harvestDiff(dboNoRecords: String?): Boolean =
    if (dboNoRecords == null) true
    else !harvested.isIsomorphicWith(safeParseRDF(dboNoRecords, Lang.TURTLE))

internal fun Model.recursiveBlankNodeSkolem(baseURI: String): Model {
    val anonSubjects = listSubjects().toList().filter { it.isAnon }
    return if (anonSubjects.isEmpty()) this
    else {
        anonSubjects
            .filter { it.doesNotContainAnon() }
            .forEach {
                ResourceUtils.renameResource(it, "$baseURI/.well-known/skolem/${it.createSkolemID()}")
            }
        this.recursiveBlankNodeSkolem(baseURI)
    }
}

private fun Resource.doesNotContainAnon(): Boolean =
    listProperties().toList()
        .filter { it.isResourceProperty() }
        .map { it.resource }
        .filter { it.listProperties().toList().size > 0 }
        .none { it.isAnon }

private fun Resource.createSkolemID(): String =
    createIdFromString(
        listProperties().toModel()
            .createRDFResponse(Lang.N3)
            .replace("\\s".toRegex(), "")
            .toCharArray()
            .sorted()
            .toString()
    )

fun splitCollectionsFromRDF(
    harvested: Model,
    allConcepts: List<ConceptRDFModel>,
    sourceURL: String,
    organization: Organization?
): List<CollectionRDFModel> {
    val harvestedCollections = harvested.listResourcesWithProperty(RDF.type, SKOS.Collection)
        .toList()
        .excludeBlankNodeCollectionsAndConcepts(sourceURL)
        .map { collectionResource ->
            val collectionConcepts: Set<String> = collectionResource.listProperties(SKOS.member)
                .toList()
                .map { it.resource }
                .excludeBlankNodeCollectionsAndConcepts(sourceURL)
                .map { it.uri }
                .toSet()

            val collectionModelWithoutConcepts = collectionResource.extractCollectionModel()
                .recursiveBlankNodeSkolem(collectionResource.uri)

            val collectionModel = ModelFactory.createDefaultModel()
            allConcepts.filter { collectionConcepts.contains(it.resourceURI) }
                .forEach { collectionModel.add(it.harvested) }

            CollectionRDFModel(
                resourceURI = collectionResource.uri,
                harvestedWithoutConcepts = collectionModelWithoutConcepts,
                harvested = collectionModel.union(collectionModelWithoutConcepts),
                concepts = collectionConcepts
            )
        }

    return harvestedCollections.plus(generatedCollection(
        allConcepts.filterNot { it.isMemberOfAnyCollection },
        sourceURL,
        organization)
    )
}

private fun List<Resource>.excludeBlankNodeCollectionsAndConcepts(sourceURL: String): List<Resource> =
    filter {
        if (it.isURIResource) true
        else {
            LOGGER.error(
                "Failed harvest of collection or concept for $sourceURL, unable to harvest blank node collections and concepts",
                Exception("unable to harvest blank node collections and concepts")
            )
            false
        }
    }

fun splitConceptsFromRDF(harvested: Model, sourceURL: String): List<ConceptRDFModel> =
    harvested.listResourcesWithProperty(RDF.type, SKOS.Concept)
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
        property.predicate != SKOS.member && property.isResourceProperty() ->
            add(property).recursiveAddNonConceptResource(property.resource)
        property.predicate != SKOS.member -> add(property)
        property.isResourceProperty() && property.resource.isURIResource -> add(property)
        else -> this
    }

fun Resource.extractConcept(): ConceptRDFModel {
    val conceptModel = listProperties().toModel()
    conceptModel.setNsPrefixes(model.nsPrefixMap)

    listProperties().toList()
        .filter { it.isResourceProperty() }
        .forEach { conceptModel.recursiveAddNonConceptResource(it.resource) }

    return ConceptRDFModel(
        resourceURI = uri,
        harvested = conceptModel.recursiveBlankNodeSkolem(uri),
        isMemberOfAnyCollection = isMemberOfAnyCollection()
    )
}

private fun Model.recursiveAddNonConceptResource(resource: Resource): Model {
    if (resourceShouldBeAdded(resource)) {
        add(resource.listProperties())

        resource.listProperties().toList()
            .filter { it.isResourceProperty() }
            .forEach { recursiveAddNonConceptResource(it.resource) }
    }

    return this
}

private fun generatedCollection(
    concepts: List<ConceptRDFModel>,
    sourceURL: String,
    organization: Organization?
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
        concepts = conceptURIs
    )
}

private fun createModelForHarvestSourceCollection(
    collectionURI: String,
    concepts: Set<String>,
    organization: Organization?
): Model {
    val collectionModel = ModelFactory.createDefaultModel()
    collectionModel.createResource(collectionURI)
        .addProperty(RDF.type, SKOS.Collection)
        .addPublisherForGeneratedCollection(organization?.uri)
        .addLabelForGeneratedCollection(organization)
        .addMembersForGeneratedCollection(concepts)

    return collectionModel
}

private fun Resource.addPublisherForGeneratedCollection(publisherURI: String?): Resource {
    if (publisherURI != null) {
        addProperty(
            DCTerms.publisher,
            ResourceFactory.createResource(publisherURI)
        )
    }

    return this
}

private fun Resource.addLabelForGeneratedCollection(organization: Organization?): Resource {
    val nb: String? = organization?.prefLabel?.nb ?: organization?.name
    if (!nb.isNullOrBlank()) {
        val label = model.createLiteral("$nb - Begrepssamling", "nb")
        addProperty(RDFS.label, label)
    }

    val nn: String? = organization?.prefLabel?.nn ?: organization?.name
    if (!nn.isNullOrBlank()) {
        val label = model.createLiteral("$nn - Begrepssamling", "nn")
        addProperty(RDFS.label, label)
    }

    val en: String? = organization?.prefLabel?.en ?: organization?.name
    if (!en.isNullOrBlank()) {
        val label = model.createLiteral("$en - Concept collection", "en")
        addProperty(RDFS.label, label)
    }

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
    val isMemberOfAnyCollection: Boolean
)

private fun Model.resourceShouldBeAdded(resource: Resource): Boolean {
    val types = resource.listProperties(RDF.type)
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
    val askQuery = """ASK {
        ?collection a <${SKOS.Collection.uri}> .
        ?collection <${SKOS.member.uri}> <$uri> .
    }""".trimMargin()

    val query = QueryFactory.create(askQuery)
    return QueryExecutionFactory.create(query, model).execAsk()
}

fun List<ConceptRDFModel>.containsConceptsWithoutCollection(): Boolean =
    firstOrNull { !it.isMemberOfAnyCollection } != null

fun formatNowWithOsloTimeZone(): String =
    ZonedDateTime.now(ZoneId.of("Europe/Oslo"))
        .format(DateTimeFormatter.ofPattern(dateFormat))

fun Calendar.formatWithOsloTimeZone(): String =
    ZonedDateTime.from(toInstant().atZone(ZoneId.of("Europe/Oslo")))
        .format(DateTimeFormatter.ofPattern(dateFormat))

class HarvestException(url: String) : Exception("Harvest failed for $url")

