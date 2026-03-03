package no.fdk.fdk_harvester.harvester

import no.fdk.fdk_harvester.config.ApplicationProperties
import no.fdk.fdk_harvester.kafka.ResourceEventProducer
import no.fdk.fdk_harvester.model.*
import no.fdk.fdk_harvester.rdf.*
import no.fdk.fdk_harvester.rdf.createDataServiceCatalogRecordModel
import no.fdk.fdk_harvester.rdf.computeChecksum
import no.fdk.fdk_harvester.harvester.formatNowWithOsloTimeZone
import no.fdk.fdk_harvester.harvester.formatWithOsloTimeZone
import no.fdk.fdk_harvester.repository.HarvestSourceRepository
import no.fdk.fdk_harvester.repository.ResourceRepository
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

private val LOGGER = LoggerFactory.getLogger(DataServiceHarvester::class.java)

/** Harvests DCAT data service catalogs from RDF and publishes dataservice events (with FDK catalog records in the graph). */
@Service
class DataServiceHarvester(
    private val applicationProperties: ApplicationProperties,
    private val resourceRepository: ResourceRepository,
    private val resourceEventProducer: ResourceEventProducer,
    harvestSourceRepository: HarvestSourceRepository
) : BaseHarvester(harvestSourceRepository) {

    fun harvestDataServiceCatalog(source: HarvestDataSource, harvestDate: Calendar, forceUpdate: Boolean, runId: String): HarvestReport? =
        validateAndHarvest(source, harvestDate, forceUpdate, runId, "dataservice", requiresAcceptHeader = true)

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
        val updatedServices = mutableListOf<FdkIdAndUri>()
        val removedServices = mutableListOf<ResourceEntity>()
        val resourceGraphs = mutableMapOf<String, String>()
        
        val catalogPairs = splitCatalogsFromRDF(harvested, sourceUrl)
            .map { Pair(it, resourceRepository.findByIdOrNull(it.resourceURI)) }
        // Validate source ownership for all catalogs and services before filtering by change (avoids reporting 0 change when feed contains resources owned by another source)
        catalogPairs.forEach { (catalog, _) ->
            validateSourceUrl(catalog.resourceURI, harvestSource, resourceRepository.findByIdOrNull(catalog.resourceURI))
            catalog.services.forEach { service ->
                validateSourceUrl(service.resourceURI, harvestSource, resourceRepository.findByIdOrNull(service.resourceURI))
            }
        }
        catalogPairs
            .filter { forceUpdate || it.first.catalogHasChanges(it.second, computeChecksum(it.first.harvestedCatalog)) }
            .forEach {
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
                updatedCatalogs.add(catalogMeta)

                val catalogFdkUri = "${applicationProperties.dataserviceUri.substringBeforeLast("/")}/catalogs/${catalogMeta.fdkId}"
                it.first.services.forEach { service ->
                    validateSourceUrl(service.resourceURI, harvestSource, resourceRepository.findByIdOrNull(service.resourceURI))
                    val result = service.updateDBOs(harvestDate, forceUpdate, harvestSource)
                    result?.let { serviceMeta ->
                        updatedServices.add(FdkIdAndUri(fdkId = serviceMeta.fdkId, uri = serviceMeta.uri))
                        val catalogRecordModel = createDataServiceCatalogRecordModel(
                            dataserviceUri = serviceMeta.uri,
                            dataserviceFdkId = serviceMeta.fdkId,
                            catalogFdkUri = catalogFdkUri,
                            issued = serviceMeta.issued,
                            modified = serviceMeta.modified,
                            dataserviceUriBase = applicationProperties.dataserviceUri
                        )
                        val graphWithRecords = service.harvestedService.union(catalogRecordModel)
                        val graphString = graphWithRecords.createRDFResponse(Lang.TURTLE)
                        resourceGraphs[serviceMeta.fdkId] = graphString
                    }
                }
            }
        
        // Mark data services as removed if they were harvested from this source but are no longer present
        val servicesFromThisSource = resourceRepository.findAllByType(ResourceType.DATASERVICE)
            .filter { it.harvestSource.id == harvestSource.id && !it.removed }
        val currentServiceUris = splitCatalogsFromRDF(harvested, sourceUrl)
            .flatMap { it.services.map { s -> s.resourceURI } }
            .toSet()
        removedServices.addAll(
            servicesFromThisSource.filter { !currentServiceUris.contains(it.uri) }
        )
        
        removedServices.map { it.copy(removed = true) }.run { resourceRepository.saveAll(this) }
        LOGGER.debug("Harvest of $sourceUrl completed")
        
        val report = HarvestReport(
            runId = runId,
            dataSourceId = sourceId,
            dataSourceUrl = sourceUrl,
            dataType = "dataservice",
            harvestError = false,
            startTime = harvestDate.formatWithOsloTimeZone(),
            endTime = formatNowWithOsloTimeZone(),
            changedCatalogs = updatedCatalogs.map { FdkIdAndUri(fdkId = it.fdkId, uri = it.uri) },
            changedResources = updatedServices,
            removedResources = removedServices.map { FdkIdAndUri(fdkId = it.fdkId, uri = it.uri) }
        )

        if (updatedServices.isNotEmpty()) {
            resourceEventProducer.publishHarvestedEvents(
                dataType = HarvestDataType.dataservice,
                resources = updatedServices,
                resourceGraphs = resourceGraphs,
                runId = runId
            )
        }

        if (removedServices.isNotEmpty()) {
            resourceEventProducer.publishRemovedEvents(
                dataType = HarvestDataType.dataservice,
                resources = removedServices.map { FdkIdAndUri(fdkId = it.fdkId, uri = it.uri) },
                runId = runId
            )
        }

        return report
    }

    private fun DataServiceModel.updateDBOs(
        harvestDate: Calendar,
        forceUpdate: Boolean,
        harvestSource: HarvestSourceEntity
    ): ResourceEntity? {
        val dbMeta = resourceRepository.findByIdOrNull(resourceURI)
        val harvestedChecksum = computeChecksum(harvestedService)
        return when {
            dbMeta == null || dbMeta.removed || serviceHasChanges(dbMeta, harvestedChecksum) -> {
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

    private fun CatalogAndDataServiceModels.mapToResource(
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

    private fun DataServiceModel.mapToResource(
        harvestDate: Calendar,
        dbMeta: ResourceEntity?,
        checksum: String,
        harvestSource: HarvestSourceEntity
    ): ResourceEntity {
        val fdkId = dbMeta?.fdkId ?: createIdFromString(resourceURI)
        val issued = dbMeta?.issued ?: harvestDate.toInstant()

        return ResourceEntity(
            uri = resourceURI,
            type = ResourceType.DATASERVICE,
            fdkId = fdkId,
            removed = false,
            issued = issued,
            modified = harvestDate.toInstant(),
            checksum = checksum,
            harvestSource = harvestSource
        )
    }

    private fun CatalogAndDataServiceModels.catalogHasChanges(dbMeta: ResourceEntity?, harvestedChecksum: String): Boolean =
        if (dbMeta == null) true
        else harvestedChecksum != dbMeta.checksum

    private fun DataServiceModel.serviceHasChanges(dbMeta: ResourceEntity?, harvestedChecksum: String): Boolean =
        if (dbMeta == null) true
        else harvestedChecksum != dbMeta.checksum

    private fun splitCatalogsFromRDF(harvested: Model, sourceURL: String): List<CatalogAndDataServiceModels> =
        harvested.listResourcesWithProperty(RDF.type, DCAT.Catalog)
            .toList()
            .filterBlankNodeCatalogsAndServices(sourceURL)
            .map { catalogResource ->
                val catalogServices: List<DataServiceModel> = catalogResource.listProperties(DCAT.service)
                    .toList()
                    .filter { it.isResourceProperty() }
                    .map { it.resource }
                    .filterBlankNodeCatalogsAndServices(sourceURL)
                    .map { it.extractDataService() }

                val catalogModelWithoutServices = catalogResource.extractCatalogModel()
                    .recursiveBlankNodeSkolem(catalogResource.uri)

                val servicesUnion = ModelFactory.createDefaultModel()
                catalogServices.forEach { servicesUnion.add(it.harvestedService) }

                CatalogAndDataServiceModels(
                    resourceURI = catalogResource.uri,
                    harvestedCatalog = catalogModelWithoutServices.union(servicesUnion),
                    harvestedCatalogWithoutServices = catalogModelWithoutServices,
                    services = catalogServices
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

    private fun Resource.extractDataService(): DataServiceModel {
        val serviceModel = listProperties().toModel()
        serviceModel.setNsPrefixes(model.nsPrefixMap)

        listProperties().toList()
            .filter { it.isResourceProperty() }
            .forEach { serviceModel.recursiveAddNonDataServiceResource(it.resource) }

        return DataServiceModel(
            resourceURI = uri,
            harvestedService = serviceModel.recursiveBlankNodeSkolem(uri)
        )
    }

    private fun List<Resource>.filterBlankNodeCatalogsAndServices(sourceURL: String): List<Resource> =
        filter {
            if (it.isURIResource) true
            else {
                LOGGER.warn("Blank node catalog or data service filtered when harvesting $sourceURL")
                false
            }
        }

    private fun Model.addCatalogProperties(property: Statement): Model =
        when {
            property.predicate != DCAT.service && property.isResourceProperty() ->
                add(property).recursiveAddNonDataServiceResource(property.resource)
            property.predicate != DCAT.service -> add(property)
            property.isResourceProperty() && property.resource.isURIResource -> add(property)
            else -> this
        }

    private fun Model.recursiveAddNonDataServiceResource(resource: Resource): Model {
        if (resourceShouldBeAdded(resource)) {
            add(resource.listProperties())

            resource.listProperties().toList()
                .filter { it.isResourceProperty() }
                .forEach { recursiveAddNonDataServiceResource(it.resource) }
        }

        return this
    }

    private fun Model.resourceShouldBeAdded(resource: Resource): Boolean {
        val types = resource.listProperties(RDF.type)
            .toList()
            .map { it.`object` }

        return when {
            types.contains(DCAT.DataService) -> false
            !resource.isURIResource -> true
            containsTriple("<${resource.uri}>", "a", "?o") -> false
            else -> true
        }
    }

    // Data classes
    private data class CatalogAndDataServiceModels(
        val resourceURI: String,
        val harvestedCatalog: Model,
        val harvestedCatalogWithoutServices: Model,
        val services: List<DataServiceModel>
    )

    private data class DataServiceModel(
        val resourceURI: String,
        val harvestedService: Model
    )
}
