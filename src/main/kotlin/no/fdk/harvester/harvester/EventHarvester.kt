package no.fdk.harvester.harvester

import no.fdk.harvester.adapter.DefaultOrganizationsAdapter
import no.fdk.harvester.config.ApplicationProperties
import no.fdk.harvester.kafka.ResourceEventProducer
import no.fdk.harvester.model.FdkIdAndUri
import no.fdk.harvester.model.HarvestDataSource
import no.fdk.harvester.model.HarvestReport
import no.fdk.harvester.model.HarvestSourceEntity
import no.fdk.harvester.model.Organization
import no.fdk.harvester.model.ResourceEntity
import no.fdk.harvester.model.ResourceType
import no.fdk.harvester.rdf.CV
import no.fdk.harvester.rdf.DCATNO
import no.fdk.harvester.rdf.computeChecksum
import no.fdk.harvester.rdf.containsTriple
import no.fdk.harvester.rdf.createCatalogRecordModel
import no.fdk.harvester.rdf.createRDFResponse
import no.fdk.harvester.repository.HarvestSourceRepository
import no.fdk.harvester.repository.ResourceRepository
import org.apache.jena.query.QueryExecutionFactory
import org.apache.jena.query.QueryFactory
import org.apache.jena.rdf.model.Model
import org.apache.jena.rdf.model.ModelFactory
import org.apache.jena.rdf.model.Resource
import org.apache.jena.rdf.model.Statement
import org.apache.jena.riot.Lang
import org.apache.jena.vocabulary.DCAT
import org.apache.jena.vocabulary.RDF
import org.springframework.data.repository.findByIdOrNull
import org.springframework.stereotype.Service
import java.util.Calendar
import no.fdk.harvest.DataType as HarvestDataType

/** Harvests event catalogs from RDF and publishes event events (with FDK catalog records in the graph). */
@Service
class EventHarvester(
    private val applicationProperties: ApplicationProperties,
    private val orgAdapter: DefaultOrganizationsAdapter,
    resourceRepository: ResourceRepository,
    private val resourceEventProducer: ResourceEventProducer,
    harvestSourceRepository: HarvestSourceRepository,
) : BaseHarvester(harvestSourceRepository, resourceRepository) {
    fun harvestEvents(
        source: HarvestDataSource,
        harvestDate: Calendar,
        forceUpdate: Boolean,
        runId: String,
    ): HarvestReport? = validateAndHarvest(source, harvestDate, forceUpdate, runId, "event", requiresAcceptHeader = true)

    override fun updateDB(
        harvested: Model,
        source: HarvestDataSource,
        harvestDate: Calendar,
        forceUpdate: Boolean,
        runId: String,
        dataType: String,
        harvestSource: HarvestSourceEntity,
    ): HarvestReport {
        val sourceId = source.id!!
        val sourceUrl = source.url!!
        val publisherId = source.publisherId
        val allEvents = splitEventsFromRDF(harvested, sourceUrl)
        val organization =
            if (publisherId != null && allEvents.containsFreeEvents()) {
                orgAdapter.getOrganization(publisherId)
            } else {
                null
            }

        val catalogs = extractCatalogs(harvested, allEvents, sourceUrl, organization)
        val (updatedCatalogs, eventUriToCatalogFdkUri) =
            updateCatalogs(
                catalogs,
                harvestDate,
                forceUpdate,
                harvestSource,
            )
        val (updatedEvents, resourceGraphs) =
            updateEvents(
                allEvents,
                harvestDate,
                forceUpdate,
                harvestSource,
                eventUriToCatalogFdkUri,
            )

        val currentEventUris = allEvents.map { it.eventURI }.toSet()
        val removedEvents = findRemovedResources(ResourceType.EVENT, currentEventUris, harvestSource)

        removedEvents
            .map { it.copy(removed = true) }
            .run { resourceRepository.saveAll(this) }

        val report =
            HarvestReportBuilder.createSuccessReport(
                dataType = dataType,
                sourceId = sourceId,
                sourceUrl = sourceUrl,
                harvestDate = harvestDate,
                changedCatalogs = updatedCatalogs,
                changedResources = updatedEvents,
                removedResources = removedEvents.map { FdkIdAndUri(fdkId = it.fdkId, uri = it.uri) },
                runId = runId,
            )

        if (updatedEvents.isNotEmpty()) {
            resourceEventProducer.publishHarvestedEvents(
                dataType = HarvestDataType.event,
                resources = updatedEvents,
                resourceGraphs = resourceGraphs,
                runId = runId,
            )
        }

        if (removedEvents.isNotEmpty()) {
            resourceEventProducer.publishRemovedEvents(
                dataType = HarvestDataType.event,
                resources = removedEvents.map { FdkIdAndUri(fdkId = it.fdkId, uri = it.uri) },
                runId = runId,
            )
        }

        return report
    }

    private fun updateCatalogs(
        catalogs: List<CatalogAndEventModels>,
        harvestDate: Calendar,
        forceUpdate: Boolean,
        harvestSource: HarvestSourceEntity,
    ): Pair<List<FdkIdAndUri>, Map<String, String>> {
        val eventUriToCatalogFdkUri = mutableMapOf<String, String>()
        catalogs.forEach { catalog ->
            validateSourceUrl(
                catalog.resourceURI,
                harvestSource,
                resourceRepository.findByIdOrNull(catalog.resourceURI),
            )
        }
        val updatedCatalogs =
            catalogs
                .map { Pair(it, resourceRepository.findByIdOrNull(it.resourceURI)) }
                .filter { forceUpdate || checksumHasChanged(it.second, computeChecksum(it.first.harvestedCatalog)) }
                .map {
                    val dbMeta = it.second
                    val catalogChecksum = computeChecksum(it.first.harvestedCatalog)
                    val catalogMeta =
                        if (dbMeta == null || checksumHasChanged(dbMeta, catalogChecksum)) {
                            createResourceEntity(
                                it.first.resourceURI,
                                ResourceType.CATALOG,
                                catalogChecksum,
                                harvestDate,
                                harvestSource,
                                dbMeta,
                            ).also { updatedMeta -> resourceRepository.save(updatedMeta) }
                        } else {
                            if (forceUpdate) {
                                dbMeta
                                    .copy(
                                        checksum = catalogChecksum,
                                        modified = harvestDate.toInstant(),
                                        harvestSource = harvestSource,
                                    ).also { resourceRepository.save(it) }
                            } else {
                                dbMeta
                            }
                        }

                    val catalogFdkUri =
                        "${applicationProperties.eventUri.substringBeforeLast("/")}/catalogs/${catalogMeta.fdkId}"
                    it.first.events.forEach { eventURI ->
                        eventUriToCatalogFdkUri[eventURI] = catalogFdkUri
                    }

                    FdkIdAndUri(fdkId = catalogMeta.fdkId, uri = catalogMeta.uri)
                }
        return Pair(updatedCatalogs, eventUriToCatalogFdkUri)
    }

    private fun updateEvents(
        events: List<EventRDFModel>,
        harvestDate: Calendar,
        forceUpdate: Boolean,
        harvestSource: HarvestSourceEntity,
        eventUriToCatalogFdkUri: Map<String, String>,
    ): Pair<List<FdkIdAndUri>, Map<String, String>> {
        val resourceGraphs = mutableMapOf<String, String>()
        val updatedEvents =
            events.mapNotNull { event ->
                try {
                    val dbMeta = resourceRepository.findByIdOrNull(event.eventURI)
                    validateSourceUrl(event.eventURI, harvestSource, dbMeta)
                    upsertResource(
                        uri = event.eventURI,
                        type = ResourceType.EVENT,
                        harvestedChecksum = computeChecksum(event.harvested),
                        harvestDate = harvestDate,
                        forceUpdate = forceUpdate,
                        harvestSource = harvestSource,
                        dbMeta = dbMeta,
                    )?.let { meta ->
                        val catalogFdkUri = eventUriToCatalogFdkUri[event.eventURI]

                        val catalogRecordModel =
                            createCatalogRecordModel(
                                resourceUri = meta.uri,
                                fdkId = meta.fdkId,
                                parentFdkUri = catalogFdkUri,
                                issued = meta.issued,
                                modified = meta.modified,
                                fdkUriBase = applicationProperties.eventUri,
                                missingParentLogMessage =
                                    if (catalogFdkUri == null) {
                                        "The event ${meta.uri} is missing associated catalog uri"
                                    } else {
                                        null
                                    },
                            )
                        val graphWithRecords = event.harvested.union(catalogRecordModel)
                        val graphString = graphWithRecords.createRDFResponse(Lang.TURTLE)
                        resourceGraphs[meta.fdkId] = graphString
                        FdkIdAndUri(fdkId = meta.fdkId, uri = event.eventURI)
                    }
                } catch (conflictError: HarvestSourceConflictException) {
                    logger.warn("Event skipped due to conflict when harvesting {}: {}", harvestSource.uri, conflictError.message)
                    null
                }
            }
        return Pair(updatedEvents, resourceGraphs)
    }

    private fun splitEventsFromRDF(
        harvested: Model,
        sourceURL: String,
    ): List<EventRDFModel> =
        harvested
            .listResourcesWithEventType()
            .toList()
            .excludeBlankNodes(sourceURL)
            .map { eventResource -> eventResource.extractEvent() }

    private fun extractCatalogs(
        harvested: Model,
        allEvents: List<EventRDFModel>,
        sourceURL: String,
        organization: Organization?,
    ): List<CatalogAndEventModels> {
        val harvestedCatalogs =
            harvested
                .listResourcesWithProperty(RDF.type, DCAT.Catalog)
                .toList()
                .excludeBlankNodes(sourceURL)
                .map { catalogResource ->
                    val catalogEvents: Set<String> =
                        catalogResource
                            .listProperties(DCATNO.containsEvent)
                            .toList()
                            .filter { it.isResourceProperty() }
                            .map { it.resource }
                            .excludeBlankNodes(sourceURL)
                            .map { it.uri }
                            .toSet()

                    val catalogModelWithoutEvents =
                        catalogResource
                            .extractCatalogModel()
                            .recursiveBlankNodeSkolem(catalogResource.uri)

                    val catalogModel = ModelFactory.createDefaultModel()
                    allEvents
                        .filter { catalogEvents.contains(it.eventURI) }
                        .forEach { catalogModel.add(it.harvested) }

                    CatalogAndEventModels(
                        resourceURI = catalogResource.uri,
                        harvestedCatalog = catalogModel.union(catalogModelWithoutEvents),
                        harvestedCatalogWithoutEvents = catalogModelWithoutEvents,
                        events = catalogEvents,
                    )
                }

        return harvestedCatalogs.plus(
            generatedCatalog(
                allEvents.filterNot { it.isMemberOfAnyCatalog },
                sourceURL,
                organization,
            ),
        )
    }

    private fun Model.listResourcesWithEventType(): List<Resource> {
        val events = listResourcesWithProperty(RDF.type, CV.Event).toList()
        val businessEvents = listResourcesWithProperty(RDF.type, CV.BusinessEvent).toList()
        val lifeEvents = listResourcesWithProperty(RDF.type, CV.LifeEvent).toList()
        return listOf(events, businessEvents, lifeEvents).flatten()
    }

    private fun List<Resource>.excludeBlankNodes(sourceURL: String): List<Resource> =
        filter {
            if (it.isURIResource) {
                true
            } else {
                logger.warn("Blank node resource filtered when harvesting $sourceURL")
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

        listProperties()
            .toList()
            .filter { it.isResourceProperty() }
            .forEach { eventModel.recursiveAddNonEventResource(it.resource) }

        return EventRDFModel(
            eventURI = uri,
            harvested = eventModel.recursiveBlankNodeSkolem(uri),
            isMemberOfAnyCatalog = isMemberOfAnyCatalog(),
        )
    }

    private fun Model.addCatalogProperties(property: Statement): Model =
        when {
            property.predicate != DCATNO.containsEvent && property.isResourceProperty() -> {
                add(property).recursiveAddNonEventResource(property.resource)
            }

            property.predicate != DCATNO.containsEvent -> {
                add(property)
            }

            property.isResourceProperty() && property.resource.isURIResource -> {
                add(property)
            }

            else -> {
                this
            }
        }

    private fun Model.recursiveAddNonEventResource(resource: Resource): Model {
        if (resourceShouldBeAdded(resource)) {
            add(resource.listProperties())

            resource
                .listProperties()
                .toList()
                .filter { it.isResourceProperty() }
                .forEach { recursiveAddNonEventResource(it.resource) }
        }

        return this
    }

    private fun Model.resourceShouldBeAdded(resource: Resource): Boolean {
        val types =
            resource
                .listProperties(RDF.type)
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
        val askQuery =
            """ASK {
            ?catalog a <${DCAT.Catalog.uri}> .
            ?catalog <${DCATNO.containsEvent.uri}> <$uri> .
        }
            """.trimMargin()

        val query = QueryFactory.create(askQuery)
        return QueryExecutionFactory.create(query, model).execAsk()
    }

    private fun List<EventRDFModel>.containsFreeEvents(): Boolean = firstOrNull { !it.isMemberOfAnyCatalog } != null

    private fun generatedCatalog(
        events: List<EventRDFModel>,
        sourceURL: String,
        organization: Organization?,
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
            events = eventURIs,
        )
    }

    private fun createModelForHarvestSourceCatalog(
        catalogURI: String,
        events: Set<String>,
        organization: Organization?,
    ): Model {
        val catalogModel = ModelFactory.createDefaultModel()
        catalogModel
            .createResource(catalogURI)
            .addProperty(RDF.type, DCAT.Catalog)
            .addPublisherForGeneratedCatalog(organization?.uri)
            .addLabelForGeneratedCatalog(organization, "Hendelseskatalog", "Event catalog")
            .addEventsForGeneratedCatalog(events)

        return catalogModel
    }

    private fun Resource.addEventsForGeneratedCatalog(events: Set<String>): Resource {
        events.forEach { addProperty(DCATNO.containsEvent, model.createResource(it)) }
        return this
    }

    private data class CatalogAndEventModels(
        val resourceURI: String,
        val harvestedCatalog: Model,
        val harvestedCatalogWithoutEvents: Model,
        val events: Set<String>,
    )

    private data class EventRDFModel(
        val eventURI: String,
        val harvested: Model,
        val isMemberOfAnyCatalog: Boolean,
    )
}
