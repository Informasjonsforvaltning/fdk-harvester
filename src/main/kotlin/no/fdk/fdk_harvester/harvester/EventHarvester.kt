package no.fdk.fdk_harvester.harvester

import no.fdk.fdk_harvester.adapter.DefaultOrganizationsAdapter
import no.fdk.fdk_harvester.config.ApplicationProperties
import no.fdk.fdk_harvester.kafka.ResourceEventProducer
import no.fdk.fdk_harvester.model.*
import no.fdk.fdk_harvester.rdf.*
import no.fdk.fdk_harvester.rdf.createEventCatalogRecordModel
import no.fdk.fdk_harvester.repository.HarvestSourceRepository
import no.fdk.fdk_harvester.repository.ResourceRepository
import no.fdk.fdk_harvester.rdf.computeChecksum
import no.fdk.fdk_harvester.harvester.formatNowWithOsloTimeZone
import no.fdk.fdk_harvester.harvester.formatWithOsloTimeZone
import no.fdk.harvest.DataType as HarvestDataType
import org.apache.jena.query.QueryExecutionFactory
import org.apache.jena.query.QueryFactory
import org.apache.jena.rdf.model.Model
import org.apache.jena.rdf.model.ModelFactory
import org.apache.jena.rdf.model.Resource
import org.apache.jena.rdf.model.ResourceFactory
import org.apache.jena.rdf.model.Statement
import org.apache.jena.riot.Lang
import org.apache.jena.vocabulary.DCAT
import org.apache.jena.vocabulary.DCTerms
import org.apache.jena.vocabulary.RDF
import org.apache.jena.vocabulary.RDFS
import org.slf4j.LoggerFactory
import org.springframework.data.repository.findByIdOrNull
import org.springframework.stereotype.Service
import java.util.*

private val LOGGER = LoggerFactory.getLogger(EventHarvester::class.java)

/** Harvests event catalogs from RDF and publishes event events (with FDK catalog records in the graph). */
@Service
class EventHarvester(
    private val applicationProperties: ApplicationProperties,
    private val orgAdapter: DefaultOrganizationsAdapter,
    private val resourceRepository: ResourceRepository,
    private val resourceEventProducer: ResourceEventProducer,
    harvestSourceRepository: HarvestSourceRepository
) : BaseHarvester(harvestSourceRepository) {

    fun harvestEvents(source: HarvestDataSource, harvestDate: Calendar, forceUpdate: Boolean, runId: String): HarvestReport? =
        validateAndHarvest(source, harvestDate, forceUpdate, runId, "event", requiresAcceptHeader = true)

    override fun updateDB(
        harvested: Model,
        source: HarvestDataSource,
        harvestDate: Calendar,
        forceUpdate: Boolean,
        runId: String,
        dataType: String,
        harvestSource: HarvestSourceEntity
    ): HarvestReport {
        val sourceId = source.id!!
        val sourceUrl = source.url!!
        val publisherId = source.publisherId
        val allEvents = splitEventsFromRDF(harvested, sourceUrl)
        val organization = if (publisherId != null && allEvents.containsFreeEvents()) {
            orgAdapter.getOrganization(publisherId)
        } else null

        val catalogs = splitCatalogsFromRDF(harvested, allEvents, sourceUrl, organization)
        val (updatedCatalogs, eventUriToCatalogFdkUri) = updateCatalogs(catalogs, harvestDate, forceUpdate, harvestSource)
        val (updatedEvents, resourceGraphs) = updateEvents(allEvents, harvestDate, forceUpdate, harvestSource, eventUriToCatalogFdkUri)

        // Mark events as removed if they were harvested from this source but are no longer present
        val eventsFromThisSource = resourceRepository.findAllByType(ResourceType.EVENT)
            .filter { it.harvestSource.id == harvestSource.id && !it.removed }
        val currentEventUris = allEvents.map { it.eventURI }.toSet()
        val removedEvents = eventsFromThisSource.filter { !currentEventUris.contains(it.uri) }
        
        removedEvents.map { it.copy(removed = true) }
            .run { resourceRepository.saveAll(this) }

        val report = HarvestReport(
            runId = runId,
            dataSourceId = sourceId,
            dataSourceUrl = sourceUrl,
            dataType = "event",
            harvestError = false,
            startTime = harvestDate.formatWithOsloTimeZone(),
            endTime = formatNowWithOsloTimeZone(),
            changedCatalogs = updatedCatalogs,
            changedResources = updatedEvents,
            removedResources = removedEvents.map { FdkIdAndUri(fdkId = it.fdkId, uri = it.uri) }
        )

        if (updatedEvents.isNotEmpty()) {
            resourceEventProducer.publishHarvestedEvents(
                dataType = HarvestDataType.event,
                resources = updatedEvents,
                resourceGraphs = resourceGraphs,
                runId = runId
            )
        }

        if (removedEvents.isNotEmpty()) {
            resourceEventProducer.publishRemovedEvents(
                dataType = HarvestDataType.event,
                resources = removedEvents.map { FdkIdAndUri(fdkId = it.fdkId, uri = it.uri) },
                runId = runId
            )
        }

        return report
    }

    private fun updateCatalogs(catalogs: List<CatalogAndEventModels>, harvestDate: Calendar, forceUpdate: Boolean, harvestSource: HarvestSourceEntity): Pair<List<FdkIdAndUri>, Map<String, String>> {
        val eventUriToCatalogFdkUri = mutableMapOf<String, String>()
        // Validate source ownership for all catalogs and events before filtering by change (avoids reporting 0 change when feed contains resources owned by another source)
        catalogs.forEach { catalog ->
            validateSourceUrl(catalog.resourceURI, harvestSource, resourceRepository.findByIdOrNull(catalog.resourceURI))
            catalog.events.forEach { eventURI ->
                validateSourceUrl(eventURI, harvestSource, resourceRepository.findByIdOrNull(eventURI))
            }
        }
        val updatedCatalogs = catalogs
            .map { Pair(it, resourceRepository.findByIdOrNull(it.resourceURI)) }
            .filter { forceUpdate || it.first.catalogHasChanges(it.second, computeChecksum(it.first.harvestedCatalog)) }
            .map {
                val dbMeta = it.second
                validateSourceUrl(it.first.resourceURI, harvestSource, dbMeta)
                val catalogChecksum = computeChecksum(it.first.harvestedCatalog)
                val catalogMeta = if (dbMeta == null || it.first.catalogHasChanges(dbMeta, catalogChecksum)) {
                    it.first.mapToResource(harvestDate, dbMeta, catalogChecksum, harvestSource)
                        .also { updatedMeta -> resourceRepository.save(updatedMeta) }
                } else {
                    if (forceUpdate) {
                        dbMeta.copy(checksum = catalogChecksum, modified = harvestDate.toInstant(), harvestSource = harvestSource)
                            .also { resourceRepository.save(it) }
                    } else {
                        dbMeta
                    }
                }

                val catalogFdkUri = "${applicationProperties.eventUri.substringBeforeLast("/")}/catalogs/${catalogMeta.fdkId}"
                it.first.events.forEach { eventURI ->
                    addIsPartOfToEvents(eventURI, catalogMeta.uri)
                    eventUriToCatalogFdkUri[eventURI] = catalogFdkUri
                }

                FdkIdAndUri(fdkId = catalogMeta.fdkId, uri = catalogMeta.uri)
            }
        return Pair(updatedCatalogs, eventUriToCatalogFdkUri)
    }

    private fun CatalogAndEventModels.mapToResource(
        harvestDate: Calendar,
        dbMeta: ResourceEntity?,
        checksum: String,
        harvestSource: HarvestSourceEntity
    ): ResourceEntity {
        val catalogURI = resourceURI
        val fdkId = dbMeta?.fdkId ?: createIdFromString(catalogURI)
        val issued = dbMeta?.issued ?: harvestDate.toInstant()

        return ResourceEntity(
            uri = catalogURI,
            type = ResourceType.CATALOG,
            fdkId = fdkId,
            removed = false,
            issued = issued,
            modified = harvestDate.toInstant(),
            checksum = checksum,
            harvestSource = harvestSource
        )
    }

    private fun updateEvents(
        events: List<EventRDFModel>,
        harvestDate: Calendar,
        forceUpdate: Boolean,
        harvestSource: HarvestSourceEntity,
        eventUriToCatalogFdkUri: Map<String, String>
    ): Pair<List<FdkIdAndUri>, Map<String, String>> {
        val resourceGraphs = mutableMapOf<String, String>()
        val updatedEvents = events.mapNotNull {
            it.updateDBOs(harvestDate, forceUpdate, harvestSource)
                ?.let { meta ->
                    val catalogFdkUri = eventUriToCatalogFdkUri[it.eventURI]
                    val graphWithRecords = if (catalogFdkUri != null) {
                        val catalogRecordModel = createEventCatalogRecordModel(
                            eventUri = meta.uri,
                            eventFdkId = meta.fdkId,
                            catalogFdkUri = catalogFdkUri,
                            issued = meta.issued,
                            modified = meta.modified,
                            eventUriBase = applicationProperties.eventUri
                        )
                        it.harvested.union(catalogRecordModel)
                    } else {
                        it.harvested
                    }
                    val graphString = graphWithRecords.createRDFResponse(Lang.TURTLE)
                    resourceGraphs[meta.fdkId] = graphString
                    FdkIdAndUri(fdkId = meta.fdkId, uri = it.eventURI)
                }
        }
        return Pair(updatedEvents, resourceGraphs)
    }

    private fun EventRDFModel.updateDBOs(harvestDate: Calendar, forceUpdate: Boolean, harvestSource: HarvestSourceEntity): ResourceEntity? {
        val dbMeta = resourceRepository.findByIdOrNull(eventURI)
        validateSourceUrl(eventURI, harvestSource, dbMeta)
        val harvestedChecksum = computeChecksum(harvested)
        return when {
            dbMeta == null || dbMeta.removed || hasChanges(dbMeta, harvestedChecksum) -> {
                val updatedMeta = mapToResource(harvestDate, dbMeta, harvestedChecksum, harvestSource)
                resourceRepository.save(updatedMeta)
                updatedMeta
            }
            forceUpdate -> {
                val updatedMeta = dbMeta.copy(checksum = harvestedChecksum, modified = harvestDate.toInstant(), harvestSource = harvestSource)
                resourceRepository.save(updatedMeta)
                updatedMeta
            }
            else -> null
        }
    }

    private fun EventRDFModel.mapToResource(
        harvestDate: Calendar,
        dbMeta: ResourceEntity?,
        checksum: String,
        harvestSource: HarvestSourceEntity
    ): ResourceEntity {
        val fdkId = dbMeta?.fdkId ?: createIdFromString(eventURI)
        val issued = dbMeta?.issued ?: harvestDate.toInstant()

        return ResourceEntity(
            uri = eventURI,
            type = ResourceType.EVENT,
            fdkId = fdkId,
            removed = false,
            issued = issued,
            modified = harvestDate.toInstant(),
            checksum = checksum,
            harvestSource = harvestSource
        )
    }

    private fun getEventsRemovedThisHarvest(events: List<String>, harvestSource: HarvestSourceEntity): List<ResourceEntity> =
        resourceRepository.findAllByType(ResourceType.EVENT)
            .filter { it.harvestSource.id == harvestSource.id && !it.removed && !events.contains(it.uri) }

    private fun addIsPartOfToEvents(eventURI: String, catalogURI: String) {
        // Note: isPartOf relationship tracking removed - using harvestSource instead
        // This method kept for compatibility but does nothing
    }

    private fun CatalogAndEventModels.catalogHasChanges(dbMeta: ResourceEntity?, harvestedChecksum: String): Boolean =
        if (dbMeta == null) true
        else harvestedChecksum != dbMeta.checksum

    private fun EventRDFModel.hasChanges(dbMeta: ResourceEntity?, harvestedChecksum: String): Boolean =
        if (dbMeta == null) true
        else harvestedChecksum != dbMeta.checksum

    private fun splitEventsFromRDF(harvested: Model, sourceURL: String): List<EventRDFModel> =
        harvested.listResourcesWithEventType()
            .toList()
            .filterBlankNodeEvents(sourceURL)
            .map { eventResource -> eventResource.extractEvent() }

    private fun splitCatalogsFromRDF(harvested: Model, allEvents: List<EventRDFModel>, sourceURL: String, organization: Organization?): List<CatalogAndEventModels> {
        val harvestedCatalogs = harvested.listResourcesWithProperty(RDF.type, DCAT.Catalog)
            .toList()
            .filterBlankNodeCatalogsAndEvents(sourceURL)
            .map { catalogResource ->
                val catalogEvents: Set<String> = catalogResource.listProperties(DCATNO.containsEvent)
                    .toList()
                    .filter { it.isResourceProperty() }
                    .map { it.resource }
                    .filterBlankNodeCatalogsAndEvents(sourceURL)
                    .map { it.uri }
                    .toSet()

                val catalogModelWithoutEvents = catalogResource.extractCatalogModel()
                    .recursiveBlankNodeSkolem(catalogResource.uri)

                val catalogModel = ModelFactory.createDefaultModel()
                allEvents.filter { catalogEvents.contains(it.eventURI) }
                    .forEach { catalogModel.add(it.harvested) }

                CatalogAndEventModels(
                    resourceURI = catalogResource.uri,
                    harvestedCatalog = catalogModel.union(catalogModelWithoutEvents),
                    harvestedCatalogWithoutEvents = catalogModelWithoutEvents,
                    events = catalogEvents
                )
            }

        return harvestedCatalogs.plus(generatedCatalog(
            allEvents.filterNot { it.isMemberOfAnyCatalog },
            sourceURL,
            organization)
        )
    }

    private fun Model.listResourcesWithEventType(): List<Resource> {
        val events = listResourcesWithProperty(RDF.type, CV.Event).toList()
        val businessEvents = listResourcesWithProperty(RDF.type, CV.BusinessEvent).toList()
        val lifeEvents = listResourcesWithProperty(RDF.type, CV.LifeEvent).toList()
        return listOf(events, businessEvents, lifeEvents).flatten()
    }

    private fun List<Resource>.filterBlankNodeEvents(sourceURL: String): List<Resource> =
        filter {
            if (it.isURIResource) true
            else {
                LOGGER.warn("Blank node event filtered when harvesting $sourceURL")
                false
            }
        }

    private fun List<Resource>.filterBlankNodeCatalogsAndEvents(sourceURL: String): List<Resource> =
        filter {
            if (it.isURIResource) true
            else {
                LOGGER.warn("Blank node catalog or event filtered when harvesting $sourceURL")
                false
            }
        }

    private fun Resource.extractCatalogModel(): Model {
        val catalogModelWithoutEvents = ModelFactory.createDefaultModel()
        catalogModelWithoutEvents.setNsPrefixes(model.nsPrefixMap)
        listProperties()
            .toList()
            .forEach { catalogModelWithoutEvents.addCatalogProperties(it) }
        return catalogModelWithoutEvents
    }

    private fun Resource.extractEvent(): EventRDFModel {
        val eventModel = listProperties().toModel()
        eventModel.setNsPrefixes(model.nsPrefixMap)

        listProperties().toList()
            .filter { it.isResourceProperty() }
            .forEach { eventModel.recursiveAddNonEventResource(it.resource) }

        return EventRDFModel(
            eventURI = uri,
            harvested = eventModel.recursiveBlankNodeSkolem(uri),
            isMemberOfAnyCatalog = isMemberOfAnyCatalog()
        )
    }

    private fun Model.addCatalogProperties(property: Statement): Model =
        when {
            property.predicate != DCATNO.containsEvent && property.isResourceProperty() ->
                add(property).recursiveAddNonEventResource(property.resource)
            property.predicate != DCATNO.containsEvent -> add(property)
            property.isResourceProperty() && property.resource.isURIResource -> add(property)
            else -> this
        }

    private fun Model.recursiveAddNonEventResource(resource: Resource): Model {
        if (resourceShouldBeAdded(resource)) {
            add(resource.listProperties())

            resource.listProperties().toList()
                .filter { it.isResourceProperty() }
                .forEach { recursiveAddNonEventResource(it.resource) }
        }

        return this
    }

    private fun Model.resourceShouldBeAdded(resource: Resource): Boolean {
        val types = resource.listProperties(RDF.type)
            .toList()
            .map { it.`object` }

        return when {
            types.contains(CV.Event) -> false
            types.contains(CV.BusinessEvent) -> false
            types.contains(CV.LifeEvent) -> false
            !resource.isURIResource -> true
            containsTriple("<${resource.uri}>", "a", "?o") -> false
            else -> true
        }
    }

    private fun Resource.isMemberOfAnyCatalog(): Boolean {
        val askQuery = """ASK {
            ?catalog a <${DCAT.Catalog.uri}> .
            ?catalog <${DCATNO.containsEvent.uri}> <$uri> .
        }""".trimMargin()

        val query = QueryFactory.create(askQuery)
        return QueryExecutionFactory.create(query, model).execAsk()
    }

    private fun List<EventRDFModel>.containsFreeEvents(): Boolean =
        firstOrNull { !it.isMemberOfAnyCatalog } != null

    private fun generatedCatalog(
        events: List<EventRDFModel>,
        sourceURL: String,
        organization: Organization?
    ): CatalogAndEventModels {
        val eventURIs = events.map { it.eventURI }.toSet()
        val generatedCatalogURI = "$sourceURL#GeneratedCatalog"
        val catalogModelWithoutEvents = createModelForHarvestSourceCatalog(generatedCatalogURI, eventURIs, organization)

        val catalogModel = ModelFactory.createDefaultModel()
        events.forEach { catalogModel.add(it.harvested) }

        return CatalogAndEventModels(
            resourceURI = generatedCatalogURI,
            harvestedCatalog = catalogModel.union(catalogModelWithoutEvents),
            harvestedCatalogWithoutEvents = catalogModelWithoutEvents,
            events = eventURIs
        )
    }

    private fun createModelForHarvestSourceCatalog(
        catalogURI: String,
        events: Set<String>,
        organization: Organization?
    ): Model {
        val catalogModel = ModelFactory.createDefaultModel()
        catalogModel.createResource(catalogURI)
            .addProperty(RDF.type, DCAT.Catalog)
            .addPublisherForGeneratedCatalog(organization?.uri)
            .addLabelForGeneratedCatalog(organization)
            .addEventsForGeneratedCatalog(events)

        return catalogModel
    }

    private fun Resource.addPublisherForGeneratedCatalog(publisherURI: String?): Resource {
        if (publisherURI != null) {
            addProperty(
                DCTerms.publisher,
                ResourceFactory.createResource(publisherURI)
            )
        }
        return this
    }

    private fun Resource.addLabelForGeneratedCatalog(organization: Organization?): Resource {
        val nb: String? = organization?.prefLabel?.nb ?: organization?.name
        if (!nb.isNullOrBlank()) {
            val label = model.createLiteral("$nb - Hendelseskatalog", "nb")
            addProperty(RDFS.label, label)
        }

        val nn: String? = organization?.prefLabel?.nn ?: organization?.name
        if (!nn.isNullOrBlank()) {
            val label = model.createLiteral("$nn - Hendelseskatalog", "nn")
            addProperty(RDFS.label, label)
        }

        val en: String? = organization?.prefLabel?.en ?: organization?.name
        if (!en.isNullOrBlank()) {
            val label = model.createLiteral("$en - Event catalog", "en")
            addProperty(RDFS.label, label)
        }

        return this
    }

    private fun Resource.addEventsForGeneratedCatalog(events: Set<String>): Resource {
        events.forEach { addProperty(DCATNO.containsEvent, model.createResource(it)) }
        return this
    }

    // Data classes
    private data class CatalogAndEventModels(
        val resourceURI: String,
        val harvestedCatalog: Model,
        val harvestedCatalogWithoutEvents: Model,
        val events: Set<String>
    )

    private data class EventRDFModel(
        val eventURI: String,
        val harvested: Model,
        val isMemberOfAnyCatalog: Boolean
    )
}
