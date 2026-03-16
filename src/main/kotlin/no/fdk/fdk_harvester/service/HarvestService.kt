package no.fdk.fdk_harvester.service

import no.fdk.fdk_harvester.harvester.ConceptHarvester
import no.fdk.fdk_harvester.harvester.DataServiceHarvester
import no.fdk.fdk_harvester.harvester.DatasetHarvester
import no.fdk.fdk_harvester.harvester.EventHarvester
import no.fdk.fdk_harvester.harvester.InformationModelHarvester
import no.fdk.fdk_harvester.harvester.ServiceHarvester
import no.fdk.fdk_harvester.harvester.HarvestReportBuilder
import no.fdk.fdk_harvester.model.FdkIdAndUri
import no.fdk.fdk_harvester.model.HarvestDataSource
import no.fdk.fdk_harvester.model.HarvestReport
import no.fdk.fdk_harvester.model.ResourceEntity
import no.fdk.fdk_harvester.model.ResourceType
import no.fdk.fdk_harvester.repository.HarvestSourceRepository
import no.fdk.fdk_harvester.repository.ResourceRepository
import no.fdk.harvest.DataType
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.time.Instant
import java.util.Calendar

/**
 * Orchestrates harvest execution and resource deletion for FDK data sources.
 *
 * Dispatches [executeHarvest] to the appropriate harvester by [DataType], and handles
 * [markResourcesAsDeleted] for a given source URL (marks matching resources as removed and publishes removed events).
 */
@Service
open class HarvestService(
    private val conceptHarvester: ConceptHarvester?,
    private val datasetHarvester: DatasetHarvester?,
    private val dataServiceHarvester: DataServiceHarvester?,
    private val informationModelHarvester: InformationModelHarvester?,
    private val serviceHarvester: ServiceHarvester?,
    private val eventHarvester: EventHarvester?,
    private val resourceRepository: ResourceRepository,
    private val harvestSourceRepository: HarvestSourceRepository
) : HarvestServiceApi {
    private fun logger(): Logger = LOGGER

    /**
     * Executes a harvest for the given parameters.
     * If the harvest source is not yet initialized (first run for this URL), the harvest is forced
     * and the source is marked initialized after success. This avoids manual config for initial deploy.
     * Returns the harvest report.
     */
    @Transactional
    override fun executeHarvest(
        dataSourceId: String,
        dataSourceUrl: String,
        dataType: DataType,
        acceptHeader: String,
        runId: String,
        forced: Boolean
    ): no.fdk.fdk_harvester.model.HarvestReport? {
        val existingSource = harvestSourceRepository.findByUri(dataSourceUrl)
        val forceBecauseNotInitialized = existingSource == null || !existingSource.initialized
        val effectiveForced = forced || forceBecauseNotInitialized
        if (forceBecauseNotInitialized) {
            logger().info("Harvest source not initialized for $dataSourceUrl, running forced harvest")
        }
        logger().info("Initiating harvest for dataSourceId: $dataSourceId, dataType: $dataType, runId: $runId, forced: $effectiveForced")

        return try {
            // Create HarvestDataSource
            val dataSource = HarvestDataSource(
                id = dataSourceId,
                url = dataSourceUrl,
                acceptHeaderValue = acceptHeader,
                dataType = dataType.name.lowercase()
            )

            val harvestDate = Calendar.getInstance()

            val report = when (dataType) {
                DataType.concept -> conceptHarvester?.harvestConceptCollection(
                    dataSource, harvestDate, effectiveForced, runId
                )
                DataType.dataset -> datasetHarvester?.harvestDatasetCatalog(
                    dataSource, harvestDate, effectiveForced, runId
                )
                DataType.dataservice -> dataServiceHarvester?.harvestDataServiceCatalog(
                    dataSource, harvestDate, effectiveForced, runId
                )
                DataType.informationmodel -> informationModelHarvester?.harvestInformationModelCatalog(
                    dataSource, harvestDate, effectiveForced, runId
                )
                DataType.publicService, DataType.service -> serviceHarvester?.harvestServices(
                    dataSource, harvestDate, effectiveForced, runId
                )
                DataType.event -> eventHarvester?.harvestEvents(
                    dataSource, harvestDate, effectiveForced, runId
                )
            }

            logger().info("Completed harvest for dataSourceId: $dataSourceId, dataType: $dataType")
            if (report != null && forceBecauseNotInitialized) {
                harvestSourceRepository.findByUri(dataSourceUrl)?.let { source ->
                    harvestSourceRepository.save(source.copy(initialized = true))
                    logger().info("Marked harvest source as initialized for $dataSourceUrl")
                }
            }
            report
        } catch (ex: Exception) {
            logger().error("Harvest failure for dataSourceId: $dataSourceId", ex)
            throw ex
        }
    }

    /**
     * Marks all resources for a given sourceUrl as deleted (removed = true) and publishes removed events.
     *
     * @param sourceUrl The URI of the harvest source
     * @param dataType The data type for the resources (used for event publishing)
     * @param dataSourceId The data source ID
     * @param runId The run ID for the harvest event (used for event publishing)
     * @return HarvestReport containing information about the deletion operation
     */
    @Transactional
    override fun markResourcesAsDeleted(
        sourceUrl: String,
        dataType: DataType,
        dataSourceId: String,
        runId: String
    ): HarvestReport {
        logger().info("Marking resources as deleted for sourceUrl: $sourceUrl, dataType: $dataType")

        val harvestSource = harvestSourceRepository.findByUri(sourceUrl)
            ?: throw IllegalArgumentException("Harvest source not found for sourceUrl: $sourceUrl")

        val harvestDate = Calendar.getInstance()
        val resources = resourceRepository.findAllByHarvestSourceId(harvestSource.id!!)
        // Filter to only resources that match the dataType, plus CATALOG and COLLECTION metadata for the source.
        // This ensures that when removeAll=true, both the resource type and its catalogs/collections are marked deleted.
        val resourcesToUpdate =
            resources.filter { resource ->
                !resource.removed &&
                    (matchesDataType(resource.type, dataType) ||
                        resource.type == ResourceType.CATALOG ||
                        resource.type == ResourceType.COLLECTION)
            }

        val removedResources = if (resourcesToUpdate.isEmpty()) {
            logger().info("No resources to mark as deleted for sourceUrl: $sourceUrl")
            emptyList()
        } else {
            val now = Instant.now()
            val updatedResources = resourcesToUpdate.map { resource ->
                // Since ResourceEntity uses val properties, we need to create a new instance
                // JPA will handle the update based on the @Id (uri)
                val updated = ResourceEntity(
                    uri = resource.uri,
                    type = resource.type,
                    fdkId = resource.fdkId,
                    removed = true,
                    issued = resource.issued,
                    modified = now,
                    checksum = resource.checksum,
                    harvestSource = resource.harvestSource
                )
                resourceRepository.save(updated)
                updated
            }

            logger().info("Marked ${updatedResources.size} resources as deleted for sourceUrl: $sourceUrl")
            updatedResources.map { FdkIdAndUri(fdkId = it.fdkId, uri = it.uri) }
        }

        // Create a report for the deletion operation
        return HarvestReportBuilder.createSuccessReport(
            dataType = dataType.name.lowercase(),
            sourceId = dataSourceId,
            sourceUrl = sourceUrl,
            harvestDate = harvestDate,
            changedCatalogs = emptyList(),
            changedResources = emptyList(),
            removedResources = removedResources,
            runId = runId
        )
    }

    @Transactional
    override fun markResourceAsDeletedByFdkId(
        fdkId: String,
        uri: String,
        dataType: DataType,
        runId: String,
        dataSourceId: String
    ): HarvestReport {
        logger().info("Marking resource as deleted for fdkId: $fdkId")

        val harvestDate = Calendar.getInstance()
        val resourcesToUpdate = resourceRepository.findAllByFdkId(fdkId)

        if (resourcesToUpdate.isNotEmpty()) {
            val now = Instant.now()
            val updatedResources = resourcesToUpdate.map { resource ->
                // Since ResourceEntity uses val properties, we need to create a new instance
                // JPA will handle the update based on the @Id (uri)
                val updated = ResourceEntity(
                    uri = resource.uri,
                    type = resource.type,
                    fdkId = resource.fdkId,
                    removed = true,
                    issued = resource.issued,
                    modified = now,
                    checksum = resource.checksum,
                    harvestSource = resource.harvestSource
                )
                resourceRepository.save(updated)
                updated
            }

            logger().info("Marked ${updatedResources.size} resources as deleted for fdkId: $fdkId")
        }

        // Create a report for the remove operation
        return HarvestReportBuilder.createSuccessReport(
            dataType = dataType.name.lowercase(),
            sourceId = dataSourceId,
            sourceUrl = null,
            harvestDate = harvestDate,
            changedCatalogs = emptyList(),
            changedResources = emptyList(),
            removedResources = listOf(FdkIdAndUri(fdkId = fdkId, uri = uri)),
            runId = runId
        )
    }

    /**
     * Checks if a ResourceType matches the given DataType.
     * CATALOG and COLLECTION are metadata types and don't match any DataType.
     */
    private fun matchesDataType(resourceType: ResourceType, dataType: DataType): Boolean {
        return when (dataType) {
            DataType.concept -> resourceType == ResourceType.CONCEPT
            DataType.dataset -> resourceType == ResourceType.DATASET
            DataType.dataservice -> resourceType == ResourceType.DATASERVICE
            DataType.informationmodel -> resourceType == ResourceType.INFORMATIONMODEL
            DataType.service, DataType.publicService -> resourceType == ResourceType.SERVICE
            DataType.event -> resourceType == ResourceType.EVENT
        }
    }

    companion object {
        private val LOGGER: Logger = LoggerFactory.getLogger(HarvestService::class.java)
    }
}

