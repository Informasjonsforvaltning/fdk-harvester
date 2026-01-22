package no.fdk.fdk_harvester.harvester

import no.fdk.fdk_harvester.config.ApplicationProperties
import no.fdk.fdk_harvester.kafka.ResourceEventProducer
import no.fdk.fdk_harvester.model.*
import no.fdk.fdk_harvester.model.ResourceEntity
import no.fdk.fdk_harvester.rdf.*
import no.fdk.fdk_harvester.rdf.DCAT3
import no.fdk.fdk_harvester.rdf.createDatasetCatalogRecordModel
import no.fdk.fdk_harvester.repository.HarvestSourceRepository
import no.fdk.fdk_harvester.repository.ResourceRepository
import no.fdk.fdk_harvester.rdf.computeChecksum
import no.fdk.harvest.DataType as HarvestDataType
import org.apache.jena.rdf.model.Model
import org.apache.jena.rdf.model.ModelFactory
import org.apache.jena.rdf.model.Resource
import org.apache.jena.rdf.model.Statement
import org.apache.jena.riot.Lang
import org.apache.jena.vocabulary.DCAT
import org.apache.jena.vocabulary.RDF
import org.slf4j.LoggerFactory
import org.springframework.data.repository.findByIdOrNull
import org.springframework.stereotype.Service
import java.util.*

/** Harvests DCAT dataset catalogs from RDF and publishes dataset events (with FDK catalog records in the graph). */
@Service
class DatasetHarvester(
    private val applicationProperties: ApplicationProperties,
    private val resourceRepository: ResourceRepository,
    harvestSourceRepository: HarvestSourceRepository,
    private val resourceEventProducer: ResourceEventProducer
) : BaseHarvester(harvestSourceRepository) {

    fun harvestDatasetCatalog(source: HarvestDataSource, harvestDate: Calendar, forceUpdate: Boolean, runId: String): HarvestReport? =
        validateAndHarvest(source, harvestDate, forceUpdate, runId, "dataset", requiresAcceptHeader = true)

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
        val updatedCatalogs = mutableListOf<ResourceEntity>()
        val updatedDatasets = mutableListOf<ResourceEntity>()
        val removedDatasets = mutableListOf<ResourceEntity>()
        val resourceGraphs = mutableMapOf<String, String>()
        extractCatalogs(harvested, sourceUrl)
            .map { Pair(it, resourceRepository.findByIdOrNull(it.resource.uri)) }
            .filter { forceUpdate || it.first.catalogHasChanges(it.second) }
            .forEach {
                val dbMeta = it.second
                validateSourceUrl(it.first.resource.uri, harvestSource, dbMeta)
                val catalogChecksum = computeChecksum(it.first.harvestedCatalog)
                val catalogMeta = if (dbMeta == null || dbMeta.type != ResourceType.CATALOG || it.first.catalogHasChanges(dbMeta)) {
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
                updatedCatalogs.add(catalogMeta)

                val catalogFdkUri = "${applicationProperties.datasetUri.substringBeforeLast("/")}/catalogs/${catalogMeta.fdkId}"
                it.first.datasets.forEach { dataset ->
                    validateSourceUrl(dataset.resource.uri, harvestSource, resourceRepository.findByIdOrNull(dataset.resource.uri))
                    val result = dataset.updateDataset(harvestDate, forceUpdate, harvestSource)
                    result?.let { datasetMeta ->
                        updatedDatasets.add(datasetMeta)
                        val catalogRecordModel = createDatasetCatalogRecordModel(
                            datasetUri = datasetMeta.uri,
                            datasetFdkId = datasetMeta.fdkId,
                            catalogFdkUri = catalogFdkUri,
                            issued = datasetMeta.issued,
                            modified = datasetMeta.modified,
                            datasetUriBase = applicationProperties.datasetUri
                        )
                        val graphWithRecords = dataset.harvestedDataset.union(catalogRecordModel)
                        val graphString = graphWithRecords.createRDFResponse(Lang.TURTLE) ?: ""
                        resourceGraphs[datasetMeta.fdkId] = graphString
                    }
                }
            }
        
        // Mark datasets as removed if they were harvested from this source but are no longer present
        val datasetsFromThisSource = resourceRepository.findAllByType(ResourceType.DATASET)
            .filter { it.harvestSource.id == harvestSource.id && !it.removed }
        val currentDatasetUris = updatedDatasets.map { it.uri }.toSet()
        removedDatasets.addAll(
            datasetsFromThisSource.filter { !currentDatasetUris.contains(it.uri) }
        )
        removedDatasets.map { it.copy(removed = true) }.run { resourceRepository.saveAll(this) }

        logger.debug("Harvest of $sourceUrl completed")
        val report = HarvestReportBuilder.createSuccessReport(
            dataType = dataType,
            sourceId = sourceId,
            sourceUrl = sourceUrl,
            harvestDate = harvestDate,
            changedCatalogs = updatedCatalogs.map { FdkIdAndUri(fdkId = it.fdkId, uri = it.uri) },
            changedResources = updatedDatasets.map { FdkIdAndUri(fdkId = it.fdkId, uri = it.uri) },
            removedResources = removedDatasets.map { FdkIdAndUri(fdkId = it.fdkId, uri = it.uri) },
            runId = runId
        )

        if (updatedDatasets.isNotEmpty()) {
            resourceEventProducer.publishHarvestedEvents(
                dataType = HarvestDataType.dataset,
                resources = updatedDatasets.map { FdkIdAndUri(fdkId = it.fdkId, uri = it.uri) },
                resourceGraphs = resourceGraphs,
                runId = runId
            )
        }

        if (removedDatasets.isNotEmpty()) {
            resourceEventProducer.publishRemovedEvents(
                dataType = HarvestDataType.dataset,
                resources = removedDatasets.map { FdkIdAndUri(fdkId = it.fdkId, uri = it.uri) },
                runId = runId
            )
        }

        return report
    }

    private fun DatasetModel.updateDataset(
        harvestDate: Calendar,
        forceUpdate: Boolean,
        harvestSource: HarvestSourceEntity
    ): ResourceEntity? {
        val dbMeta = resourceRepository.findByIdOrNull(resource.uri)
        val harvestedChecksum = computeChecksum(harvestedDataset)
        return when {
            dbMeta == null || dbMeta.removed || dbMeta.type != ResourceType.DATASET || datasetHasChanges(dbMeta) -> {
                val datasetMeta = mapToResource(harvestDate, dbMeta, harvestedChecksum, harvestSource)
                resourceRepository.save(datasetMeta)
                datasetMeta
            }
            forceUpdate -> {
                val updatedMeta = dbMeta.copy(checksum = harvestedChecksum, modified = harvestDate.toInstant(), harvestSource = harvestSource)
                resourceRepository.save(updatedMeta)
                updatedMeta
            }
            else -> null
        }
    }

    private fun CatalogAndDatasetModels.catalogHasChanges(dbMeta: ResourceEntity?): Boolean =
        if (dbMeta == null) true
        else {
            val harvestedChecksum = computeChecksum(harvestedCatalog)
            harvestedChecksum != dbMeta.checksum
        }

    private fun DatasetModel.datasetHasChanges(dbMeta: ResourceEntity?): Boolean =
        if (dbMeta == null) true
        else {
            val harvestedChecksum = computeChecksum(harvestedDataset)
            harvestedChecksum != dbMeta.checksum
        }

    private fun CatalogAndDatasetModels.mapToResource(
        harvestDate: Calendar,
        dbMeta: ResourceEntity?,
        checksum: String,
        harvestSource: HarvestSourceEntity
    ): ResourceEntity {
        val catalogURI = resource.uri
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

    private fun DatasetModel.mapToResource(
        harvestDate: Calendar,
        dbMeta: ResourceEntity?,
        checksum: String,
        harvestSource: HarvestSourceEntity
    ): ResourceEntity {
        val fdkId = dbMeta?.fdkId ?: createIdFromString(resource.uri)
        val issued = dbMeta?.issued ?: harvestDate.toInstant()

        return ResourceEntity(
            uri = resource.uri,
            type = ResourceType.DATASET,
            fdkId = fdkId,
            removed = false,
            issued = issued,
            modified = harvestDate.toInstant(),
            checksum = checksum,
            harvestSource = harvestSource
        )
    }


    private fun extractCatalogs(harvested: Model, sourceURL: String): List<CatalogAndDatasetModels> =
        harvested.listResourcesWithProperty(RDF.type, DCAT.Catalog)
            .toList()
            .filterBlankNodeCatalogsAndDatasets(sourceURL)
            .map { catalogResource ->
                val catalogDatasets: List<DatasetModel> = catalogResource.listProperties(DCAT.dataset)
                    .toList()
                    .filter { it.isResourceProperty() }
                    .map { it.resource }
                    .flatMap { it.extractDatasetsInSeries() }
                    .filter { it.isDataset() }
                    .filterBlankNodeCatalogsAndDatasets(sourceURL)
                    .map { it.extractDataset() }

                val catalogModelWithoutDatasets = catalogResource.extractCatalogModel()

                catalogModelWithoutDatasets.recursiveBlankNodeSkolem(catalogResource.uri)

                val datasetsUnion = ModelFactory.createDefaultModel()
                catalogDatasets.forEach { datasetsUnion.add(it.harvestedDataset) }

                CatalogAndDatasetModels(
                    resource = catalogResource,
                    harvestedCatalog = catalogModelWithoutDatasets.union(datasetsUnion),
                    harvestedCatalogWithoutDatasets = catalogModelWithoutDatasets,
                    datasets = catalogDatasets
                )
            }

    private fun Resource.extractCatalogModel(): Model {
        val catalogModelWithoutServices = ModelFactory.createDefaultModel()
        catalogModelWithoutServices.setNsPrefixes(model.nsPrefixMap)
        listProperties()
            .toList()
            .forEach { catalogModelWithoutServices.addCatalogProperties(it) }
        return catalogModelWithoutServices
    }

    private fun List<Resource>.filterBlankNodeCatalogsAndDatasets(sourceURL: String): List<Resource> =
        filter {
            if (it.isURIResource) true
            else {
                logger.error(
                    "Failed harvest of catalog or dataset for $sourceURL, unable to harvest blank node catalogs and dataset",
                    Exception("unable to harvest blank node catalogs and datasets")
                )
                false
            }
        }

    private fun Model.addCatalogProperties(property: Statement): Model =
        when {
            property.predicate != DCAT.dataset && property.isResourceProperty() ->
                add(property).recursiveAddNonDatasetResource(property.resource)
            property.predicate != DCAT.dataset -> add(property)
            property.isResourceProperty() && property.resource.isURIResource -> add(property)
            else -> this
        }

    private fun Resource.extractDataset(): DatasetModel {
        val datasetModel = listProperties().toModel()
        datasetModel.setNsPrefixes(model.nsPrefixMap)

        listProperties().toList()
            .filter { it.isResourceProperty() }
            .forEach { datasetModel.recursiveAddNonDatasetResource(it.resource) }

        return DatasetModel(resource = this, harvestedDataset = datasetModel.recursiveBlankNodeSkolem(uri))
    }

    private fun Model.recursiveAddNonDatasetResource(resource: Resource): Model {
        if (resourceShouldBeAdded(resource)) {
            add(resource.listProperties())

            resource.listProperties().toList()
                .filter { it.isResourceProperty() }
                .forEach { recursiveAddNonDatasetResource(it.resource) }
        }

        return this
    }

    private fun Model.resourceShouldBeAdded(resource: Resource): Boolean {
        val types = resource.listProperties(RDF.type)
            .toList()
            .map { it.`object` }

        return when {
            types.contains(DCAT.Dataset) -> false
            types.contains(DCAT3.DatasetSeries) -> false
            !resource.isURIResource -> true
            containsTriple("<${resource.uri}>", "a", "?o") -> false
            else -> true
        }
    }

    private fun Resource.extractDatasetsInSeries(): List<Resource> {
        val types = listProperties(RDF.type)
            .toList()
            .map { it.`object` }

        return if (types.contains(DCAT3.DatasetSeries)) {
            val datasetsInSeries = model.listResourcesWithProperty(DCAT3.inSeries, this)
                .toList()
            datasetsInSeries.add(this)
            datasetsInSeries
        } else {
            listOf(this)
        }
    }

    private fun Resource.isDataset(): Boolean {
        val types = listProperties(RDF.type)
            .toList()
            .map { it.`object` }

        return when {
            types.contains(DCAT.Dataset) -> true
            types.contains(DCAT3.DatasetSeries) -> true
            else -> false
        }
    }

    // Data classes from DatasetHarvestHelpers
    private data class CatalogAndDatasetModels(
        val resource: Resource,
        val harvestedCatalog: Model,
        val harvestedCatalogWithoutDatasets: Model,
        val datasets: List<DatasetModel>,
    )

    private data class DatasetModel(
        val resource: Resource,
        val harvestedDataset: Model
    )
}
