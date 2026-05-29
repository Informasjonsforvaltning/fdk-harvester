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
import no.fdk.harvester.rdf.createCatalogRecordModel
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
    resourceRepository: ResourceRepository,
    private val resourceEventProducer: ResourceEventProducer,
    private val applicationProperties: ApplicationProperties,
    harvestSourceRepository: HarvestSourceRepository,
) : BaseHarvester(harvestSourceRepository, resourceRepository) {
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
            findRemovedResources(
                ResourceType.CONCEPT,
                concepts.map { it.resourceURI }.toSet(),
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
            concepts.mapNotNull { concept ->
                try {
                    val dbMeta = resourceRepository.findByIdOrNull(concept.resourceURI)
                    validateSourceUrl(concept.resourceURI, harvestSource, dbMeta)
                    upsertResource(
                        uri = concept.resourceURI,
                        type = ResourceType.CONCEPT,
                        harvestedChecksum = computeChecksum(concept.harvested),
                        harvestDate = harvestDate,
                        forceUpdate = forceUpdate,
                        harvestSource = harvestSource,
                        dbMeta = dbMeta,
                    )?.let { meta ->
                        val graphWithRecords =
                            concept.harvested.union(
                                createCatalogRecordModel(
                                    resourceUri = concept.resourceURI,
                                    fdkId = meta.fdkId,
                                    parentFdkUri = conceptUriToCollectionFdkUri[concept.resourceURI],
                                    issued = meta.issued,
                                    modified = meta.modified,
                                    fdkUriBase = conceptUriBase,
                                    missingParentLogMessage =
                                        if (conceptUriToCollectionFdkUri[concept.resourceURI] == null) {
                                            "The concept ${concept.resourceURI} is missing associated collection uri"
                                        } else {
                                            null
                                        },
                                ),
                            )
                        val graphString = graphWithRecords.createRDFResponse(Lang.TURTLE)
                        resourceGraphs[meta.fdkId] = graphString
                        FdkIdAndUri(fdkId = meta.fdkId, uri = concept.resourceURI)
                    }
                } catch (conflictError: HarvestSourceConflictException) {
                    logger.warn("Concept skipped due to conflict when harvesting {}: {}", harvestSource.uri, conflictError.message)
                    null
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
}
