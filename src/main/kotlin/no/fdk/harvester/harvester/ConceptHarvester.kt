package no.fdk.harvester.harvester

import no.fdk.harvester.adapter.DefaultOrganizationsAdapter
import no.fdk.harvester.config.ApplicationProperties
import no.fdk.harvester.kafka.ResourceEventProducer
import no.fdk.harvester.model.FdkIdAndUri
import no.fdk.harvester.model.HarvestDataSource
import no.fdk.harvester.model.HarvestReport
import no.fdk.harvester.model.HarvestSourceEntity
import no.fdk.harvester.model.ResourceEntity
import no.fdk.harvester.model.ResourceType
import no.fdk.harvester.rdf.computeChecksum
import no.fdk.harvester.rdf.createConceptCatalogRecordModel
import no.fdk.harvester.rdf.createIdFromString
import no.fdk.harvester.rdf.createRDFResponse
import no.fdk.harvester.repository.HarvestSourceRepository
import no.fdk.harvester.repository.ResourceRepository
import org.apache.jena.rdf.model.Model
import org.apache.jena.riot.Lang
import org.springframework.data.repository.findByIdOrNull
import org.springframework.stereotype.Service
import java.util.Calendar
import no.fdk.harvest.DataType as HarvestDataType

/** Harvests concept collections from a SKOS/RDF source and publishes concept events. */
@Service
class ConceptHarvester(
    private val orgAdapter: DefaultOrganizationsAdapter,
    private val resourceRepository: ResourceRepository,
    private val resourceEventProducer: ResourceEventProducer,
    private val applicationProperties: ApplicationProperties,
    harvestSourceRepository: HarvestSourceRepository,
) : BaseHarvester(harvestSourceRepository) {
    fun harvestConceptCollection(
        source: HarvestDataSource,
        harvestDate: Calendar,
        forceUpdate: Boolean,
        runId: String,
    ): HarvestReport? = validateAndHarvest(source, harvestDate, forceUpdate, runId, "concept", requiresAcceptHeader = true)

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
        val sourceURL = source.url!!
        val publisherId = source.publisherId
        val concepts = splitConceptsFromRDF(harvested, sourceURL)

        val organization =
            if (publisherId != null && concepts.containsConceptsWithoutCollection()) {
                orgAdapter.getOrganization(publisherId)
            } else {
                null
            }

        val collections = splitCollectionsFromRDF(harvested, concepts, sourceURL, organization)
        val (updatedCollections, conceptUriToCollectionFdkUri) =
            updateCollections(
                collections,
                harvestDate,
                forceUpdate,
                harvestSource,
            )
        val updatedConcepts =
            updateConcepts(concepts, harvestDate, forceUpdate, runId, harvestSource, conceptUriToCollectionFdkUri)

        val removedConcepts =
            getConceptsRemovedThisHarvest(
                concepts.map { it.resourceURI },
                harvestSource,
            )
        removedConcepts
            .map { it.copy(removed = true) }
            .run { resourceRepository.saveAll(this) }

        if (removedConcepts.isNotEmpty()) {
            resourceEventProducer.publishRemovedEvents(
                dataType = HarvestDataType.concept,
                resources = removedConcepts.map { FdkIdAndUri(fdkId = it.fdkId, uri = it.uri) },
                runId = runId,
            )
        }

        return HarvestReportBuilder.createSuccessReport(
            dataType = dataType,
            sourceId = sourceId,
            sourceUrl = sourceURL,
            harvestDate = harvestDate,
            changedCatalogs = updatedCollections,
            changedResources = updatedConcepts,
            removedResources = removedConcepts.map { FdkIdAndUri(fdkId = it.fdkId, uri = it.uri) },
            runId = runId,
        )
    }

    private fun updateConcepts(
        concepts: List<ConceptRDFModel>,
        harvestDate: Calendar,
        forceUpdate: Boolean,
        runId: String,
        harvestSource: HarvestSourceEntity,
        conceptUriToCollectionFdkUri: Map<String, String>,
    ): List<FdkIdAndUri> {
        val resourceGraphs = mutableMapOf<String, String>()
        val conceptUriBase = applicationProperties.conceptUri
        val updatedConcepts =
            concepts.mapNotNull {
                it
                    .upsertResource(harvestDate, forceUpdate, harvestSource)
                    ?.let { meta ->
                        val graphWithRecords =
                            it.harvested.union(
                                createConceptCatalogRecordModel(
                                    conceptUri = it.resourceURI,
                                    conceptFdkId = meta.fdkId,
                                    collectionFdkUri = conceptUriToCollectionFdkUri[it.resourceURI],
                                    issued = meta.issued,
                                    modified = meta.modified,
                                    conceptUriBase = conceptUriBase,
                                ),
                            )
                        val graphString = graphWithRecords.createRDFResponse(Lang.TURTLE)
                        resourceGraphs[meta.fdkId] = graphString
                        FdkIdAndUri(fdkId = meta.fdkId, uri = it.resourceURI)
                    }
            }

        if (updatedConcepts.isNotEmpty()) {
            resourceEventProducer.publishHarvestedEvents(
                dataType = HarvestDataType.concept,
                resources = updatedConcepts,
                resourceGraphs = resourceGraphs,
                runId = runId,
            )
        }

        return updatedConcepts
    }

    private fun ConceptRDFModel.upsertResource(
        harvestDate: Calendar,
        forceUpdate: Boolean,
        harvestSource: HarvestSourceEntity,
    ): ResourceEntity? {
        try {
            val dbMeta = resourceRepository.findByIdOrNull(resourceURI)
            validateSourceUrl(resourceURI, harvestSource, dbMeta)
            val harvestedChecksum = computeChecksum(harvested)
            return when {
                dbMeta == null || dbMeta.removed || checksumHasChanged(dbMeta, harvestedChecksum) -> {
                    val updatedMeta =
                        createResourceEntity(resourceURI, ResourceType.CONCEPT, harvestedChecksum, harvestDate, harvestSource, dbMeta)
                    resourceRepository.save(updatedMeta)
                    updatedMeta
                }

                forceUpdate -> {
                    val updatedMeta =
                        dbMeta.copy(
                            checksum = harvestedChecksum,
                            harvestSource = harvestSource,
                        )
                    resourceRepository.save(updatedMeta)
                    updatedMeta
                }

                else -> {
                    null
                }
            }
        } catch (conflictError: HarvestSourceConflictException) {
            logger.warn("Concept skipped due to conflict when harvesting {}: {}", harvestSource.uri, conflictError.message)
            return null
        }
    }

    private fun updateCollections(
        collections: List<CollectionRDFModel>,
        harvestDate: Calendar,
        forceUpdate: Boolean,
        harvestSource: HarvestSourceEntity,
    ): Pair<List<FdkIdAndUri>, Map<String, String>> {
        val conceptUriToCollectionFdkUri = mutableMapOf<String, String>()
        val collectionUriBase = "${applicationProperties.conceptUri.substringBeforeLast("/")}/collections"
        collections.forEach { coll ->
            val dbMeta = resourceRepository.findByIdOrNull(coll.resourceURI)
            val fdkId = dbMeta?.fdkId ?: createIdFromString(coll.resourceURI)
            val collectionFdkUri = "$collectionUriBase/$fdkId"
            coll.concepts.forEach { conceptURI ->
                conceptUriToCollectionFdkUri.putIfAbsent(conceptURI, collectionFdkUri)
            }
        }
        collections.forEach { coll ->
            validateSourceUrl(coll.resourceURI, harvestSource, resourceRepository.findByIdOrNull(coll.resourceURI))
        }
        val updated =
            collections
                .map { Pair(it, resourceRepository.findByIdOrNull(it.resourceURI)) }
                .filter { forceUpdate || checksumHasChanged(it.second, computeChecksum(it.first.harvested)) }
                .map {
                    val dbMeta = it.second
                    val collectionChecksum = computeChecksum(it.first.harvested)
                    val collectionMeta =
                        if (dbMeta == null || checksumHasChanged(dbMeta, collectionChecksum)) {
                            createResourceEntity(
                                it.first.resourceURI,
                                ResourceType.COLLECTION,
                                collectionChecksum,
                                harvestDate,
                                harvestSource,
                                dbMeta,
                            ).also { updatedMeta -> resourceRepository.save(updatedMeta) }
                        } else {
                            if (forceUpdate) {
                                dbMeta
                                    .copy(
                                        checksum = collectionChecksum,
                                        modified = harvestDate.toInstant(),
                                        harvestSource = harvestSource,
                                    ).also { resourceRepository.save(it) }
                            } else {
                                dbMeta
                            }
                        }
                    FdkIdAndUri(fdkId = collectionMeta.fdkId, uri = collectionMeta.uri)
                }
        return Pair(updated, conceptUriToCollectionFdkUri)
    }

    private fun getConceptsRemovedThisHarvest(
        concepts: List<String>,
        harvestSource: HarvestSourceEntity,
    ): List<ResourceEntity> =
        resourceRepository
            .findAllByType(ResourceType.CONCEPT)
            .filter { it.harvestSource.id == harvestSource.id && !it.removed && !concepts.contains(it.uri) }
}
