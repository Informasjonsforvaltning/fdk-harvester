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
     * This method handles the actual harvest execution and metrics.
     * Returns the harvest report.
     */
    override fun executeHarvest(
        dataSourceId: String,
        dataSourceUrl: String,
        dataType: DataType,
        acceptHeader: String,
        runId: String,
        forced: Boolean
    ): no.fdk.fdk_harvester.model.HarvestReport? {
        logger().info("Initiating harvest for dataSourceId: $dataSourceId, dataType: $dataType, runId: $runId, forced: $forced")

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
                    dataSource, harvestDate, forced, runId
                )
                DataType.dataset -> datasetHarvester?.harvestDatasetCatalog(
                    dataSource, harvestDate, forced, runId
                )
                DataType.dataservice -> dataServiceHarvester?.harvestDataServiceCatalog(
                    dataSource, harvestDate, forced, runId
                )
                DataType.informationmodel -> informationModelHarvester?.harvestInformationModelCatalog(
                    dataSource, harvestDate, forced, runId
                )
                DataType.publicService, DataType.service -> serviceHarvester?.harvestServices(
                    dataSource, harvestDate, forced, runId
                )
                DataType.event -> eventHarvester?.harvestEvents(
                    dataSource, harvestDate, forced, runId
                )
                else -> {
                    logger().error("Unknown data type: $dataType")
                    null
                }
            }

            logger().info("Completed harvest for dataSourceId: $dataSourceId, dataType: $dataType")
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
        // Filter to only resources that match the dataType (excluding CATALOG and COLLECTION which are metadata)
        val resourcesToUpdate = resources.filter { !it.removed && matchesDataType(it.type, dataType) }

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
            else -> false
        }
    }

    companion object {
        private val LOGGER: Logger = LoggerFactory.getLogger(HarvestService::class.java)
    }
}

