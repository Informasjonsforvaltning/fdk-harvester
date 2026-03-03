package no.fdk.fdk_harvester.harvester

import no.fdk.fdk_harvester.adapter.DefaultOrganizationsAdapter
import no.fdk.fdk_harvester.config.ApplicationProperties
import no.fdk.fdk_harvester.kafka.ResourceEventProducer
import no.fdk.fdk_harvester.model.*
import no.fdk.fdk_harvester.rdf.*
import no.fdk.fdk_harvester.rdf.computeChecksum
import no.fdk.fdk_harvester.harvester.formatNowWithOsloTimeZone
import no.fdk.fdk_harvester.harvester.formatWithOsloTimeZone
import no.fdk.fdk_harvester.repository.HarvestSourceRepository
import no.fdk.fdk_harvester.repository.ResourceRepository
import no.fdk.harvest.DataType as HarvestDataType
import org.apache.jena.rdf.model.Model
import org.apache.jena.riot.Lang
import org.slf4j.LoggerFactory
import org.springframework.data.repository.findByIdOrNull
import org.springframework.stereotype.Service
import java.util.*

private val LOGGER = LoggerFactory.getLogger(ConceptHarvester::class.java)

/** Harvests concept collections from a SKOS/RDF source and publishes concept events. */
@Service
class ConceptHarvester(
    private val orgAdapter: DefaultOrganizationsAdapter,
    private val resourceRepository: ResourceRepository,
    private val resourceEventProducer: ResourceEventProducer,
    private val applicationProperties: ApplicationProperties,
    harvestSourceRepository: HarvestSourceRepository
) : BaseHarvester(harvestSourceRepository) {

    fun harvestConceptCollection(source: HarvestDataSource, harvestDate: Calendar, forceUpdate: Boolean, runId: String): HarvestReport? =
        validateAndHarvest(source, harvestDate, forceUpdate, runId, "concept", requiresAcceptHeader = true)

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
        val sourceURL = source.url!!
        val publisherId = source.publisherId
        val concepts = splitConceptsFromRDF(harvested, sourceURL)

        val organization = if (publisherId != null && concepts.containsConceptsWithoutCollection()) {
            orgAdapter.getOrganization(publisherId)
        } else null

        val collections = splitCollectionsFromRDF(harvested, concepts, sourceURL, organization)
        val (updatedCollections, conceptUriToCollectionFdkUri) = updateCollections(collections, harvestDate, forceUpdate, harvestSource)
        val updatedConcepts = updateConcepts(concepts, harvestDate, forceUpdate, runId, harvestSource, conceptUriToCollectionFdkUri)

        val removedConcepts = getConceptsRemovedThisHarvest(
            concepts.map { it.resourceURI },
            harvestSource
        )
        removedConcepts.map { it.copy(removed = true) }
            .run { resourceRepository.saveAll(this) }

        if (removedConcepts.isNotEmpty()) {
            resourceEventProducer.publishRemovedEvents(
                dataType = HarvestDataType.concept,
                resources = removedConcepts.map { FdkIdAndUri(fdkId = it.fdkId, uri = it.uri) },
                runId = runId
            )
        }

        return HarvestReport(
            runId = runId,
            dataSourceId = sourceId,
            dataSourceUrl = sourceURL,
            dataType = "concept",
            harvestError = false,
            startTime = harvestDate.formatWithOsloTimeZone(),
            endTime = formatNowWithOsloTimeZone(),
            changedCatalogs = updatedCollections,
            changedResources = updatedConcepts,
            removedResources = removedConcepts.map { FdkIdAndUri(fdkId = it.fdkId, uri = it.uri) }
        )
    }

    private fun updateConcepts(
        concepts: List<ConceptRDFModel>,
        harvestDate: Calendar,
        forceUpdate: Boolean,
        runId: String,
        harvestSource: HarvestSourceEntity,
        conceptUriToCollectionFdkUri: Map<String, String>
    ): List<FdkIdAndUri> {
        val resourceGraphs = mutableMapOf<String, String>()
        val conceptUriBase = applicationProperties.conceptUri
        val updatedConcepts = concepts.mapNotNull {
            it.updateDBOs(harvestDate, forceUpdate, harvestSource)
                ?.let { meta ->
                    val graphWithRecords = conceptUriToCollectionFdkUri[it.resourceURI]?.let { collectionFdkUri ->
                        it.harvested.union(
                            createConceptCatalogRecordModel(
                                conceptUri = it.resourceURI,
                                conceptFdkId = meta.fdkId,
                                collectionFdkUri = collectionFdkUri,
                                issued = meta.issued,
                                modified = meta.modified,
                                conceptUriBase = conceptUriBase
                            )
                        )
                    } ?: it.harvested
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
                runId = runId
            )
        }

        return updatedConcepts
    }

    private fun ConceptRDFModel.updateDBOs(harvestDate: Calendar, forceUpdate: Boolean, harvestSource: HarvestSourceEntity): ResourceEntity? {
        val dbMeta = resourceRepository.findByIdOrNull(resourceURI)
        validateSourceUrl(resourceURI, harvestSource, dbMeta)
        val harvestedChecksum = computeChecksum(harvested)
        return when {
            dbMeta == null || dbMeta.removed || conceptHasChanges(dbMeta, harvestedChecksum) -> {
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

    private fun updateCollections(
        collections: List<CollectionRDFModel>,
        harvestDate: Calendar,
        forceUpdate: Boolean,
        harvestSource: HarvestSourceEntity
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
        // Validate source ownership for all collections before filtering by change (avoids reporting 0 change when feed contains resources owned by another source)
        collections.forEach { coll ->
            validateSourceUrl(coll.resourceURI, harvestSource, resourceRepository.findByIdOrNull(coll.resourceURI))
        }
        val updated = collections
            .map { Pair(it, resourceRepository.findByIdOrNull(it.resourceURI)) }
            .filter { forceUpdate || it.first.collectionHasChanges(it.second, computeChecksum(it.first.harvested)) }
            .map {
                val dbMeta = it.second
                validateSourceUrl(it.first.resourceURI, harvestSource, dbMeta)
                val collectionChecksum = computeChecksum(it.first.harvested)
                val collectionMeta = if (dbMeta == null || it.first.collectionHasChanges(dbMeta, collectionChecksum)) {
                    it.first.mapToResource(harvestDate, dbMeta, collectionChecksum, harvestSource)
                        .also { updatedMeta -> resourceRepository.save(updatedMeta) }
                } else {
                    if (forceUpdate) {
                        dbMeta.copy(checksum = collectionChecksum, modified = harvestDate.toInstant(), harvestSource = harvestSource)
                            .also { resourceRepository.save(it) }
                    } else {
                        dbMeta
                    }
                }
                it.first.concepts.forEach { conceptURI ->
                    addIsPartOfToConcept(conceptURI, collectionMeta.uri)
                }
                FdkIdAndUri(fdkId = collectionMeta.fdkId, uri = collectionMeta.uri)
            }
        return Pair(updated, conceptUriToCollectionFdkUri)
    }

    private fun CollectionRDFModel.mapToResource(
        harvestDate: Calendar,
        dbMeta: ResourceEntity?,
        checksum: String,
        harvestSource: HarvestSourceEntity
    ): ResourceEntity {
        val collectionURI = resourceURI
        val fdkId = dbMeta?.fdkId ?: createIdFromString(collectionURI)
        val issued = dbMeta?.issued ?: harvestDate.toInstant()

        return ResourceEntity(
            uri = collectionURI,
            type = ResourceType.COLLECTION,
            fdkId = fdkId,
            removed = false,
            issued = issued,
            modified = harvestDate.toInstant(),
            checksum = checksum,
            harvestSource = harvestSource
        )
    }

    private fun ConceptRDFModel.mapToResource(
        harvestDate: Calendar,
        dbMeta: ResourceEntity?,
        checksum: String,
        harvestSource: HarvestSourceEntity
    ): ResourceEntity {
        val fdkId = dbMeta?.fdkId ?: createIdFromString(resourceURI)
        val issued = dbMeta?.issued ?: harvestDate.toInstant()

        return ResourceEntity(
            uri = resourceURI,
            type = ResourceType.CONCEPT,
            fdkId = fdkId,
            removed = false,
            issued = issued,
            modified = harvestDate.toInstant(),
            checksum = checksum,
            harvestSource = harvestSource
        )
    }

    private fun addIsPartOfToConcept(conceptURI: String, collectionURI: String) {
        // Note: isPartOf relationship tracking removed - using harvestSource instead
        // This method kept for compatibility but does nothing
    }

    private fun CollectionRDFModel.collectionHasChanges(dbMeta: ResourceEntity?, harvestedChecksum: String): Boolean =
        if (dbMeta == null) true
        else harvestedChecksum != dbMeta.checksum

    private fun ConceptRDFModel.conceptHasChanges(dbMeta: ResourceEntity?, harvestedChecksum: String): Boolean =
        if (dbMeta == null) true
        else harvestedChecksum != dbMeta.checksum

    private fun getConceptsRemovedThisHarvest(concepts: List<String>, harvestSource: HarvestSourceEntity): List<ResourceEntity> =
        resourceRepository.findAllByType(ResourceType.CONCEPT)
            .filter { it.harvestSource.id == harvestSource.id && !it.removed && !concepts.contains(it.uri) }
}
