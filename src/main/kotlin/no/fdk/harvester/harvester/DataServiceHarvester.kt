package no.fdk.harvester.harvester

import no.fdk.harvester.config.ApplicationProperties
import no.fdk.harvester.kafka.ResourceEventProducer
import no.fdk.harvester.model.FdkIdAndUri
import no.fdk.harvester.model.HarvestDataSource
import no.fdk.harvester.model.HarvestReport
import no.fdk.harvester.model.HarvestSourceEntity
import no.fdk.harvester.model.ResourceEntity
import no.fdk.harvester.model.ResourceType
import no.fdk.harvester.rdf.computeChecksum
import no.fdk.harvester.rdf.containsTriple
import no.fdk.harvester.rdf.createCatalogRecordModel
import no.fdk.harvester.rdf.createRDFResponse
import no.fdk.harvester.repository.HarvestSourceRepository
import no.fdk.harvester.repository.ResourceRepository
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

/** Harvests DCAT data service catalogs from RDF and publishes dataservice events (with FDK catalog records in the graph). */
@Service
class DataServiceHarvester(
    private val applicationProperties: ApplicationProperties,
    resourceRepository: ResourceRepository,
    private val resourceEventProducer: ResourceEventProducer,
    harvestSourceRepository: HarvestSourceRepository,
) : BaseHarvester(harvestSourceRepository, resourceRepository) {
    fun harvestDataServiceCatalog(
        source: HarvestDataSource,
        harvestDate: Calendar,
        forceUpdate: Boolean,
        runId: String,
    ): HarvestReport? = validateAndHarvest(source, harvestDate, forceUpdate, runId, "dataservice", requiresAcceptHeader = true)

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
        val updatedCatalogs = mutableListOf<ResourceEntity>()
        val updatedServices = mutableListOf<FdkIdAndUri>()
        val removedServices = mutableListOf<ResourceEntity>()
        val resourceGraphs = mutableMapOf<String, String>()

        val catalogPairs =
            extractCatalogs(harvested, sourceUrl)
                .map { Pair(it, resourceRepository.findByIdOrNull(it.resourceURI)) }
        catalogPairs.forEach { (catalog, _) ->
            validateSourceUrl(
                catalog.resourceURI,
                harvestSource,
                resourceRepository.findByIdOrNull(catalog.resourceURI),
            )
        }
        catalogPairs
            .filter { forceUpdate || checksumHasChanged(it.second, computeChecksum(it.first.harvestedCatalog)) }
            .forEach {
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
                updatedCatalogs.add(catalogMeta)

                val catalogFdkUri =
                    "${applicationProperties.dataserviceUri.substringBeforeLast("/")}/catalogs/${catalogMeta.fdkId}"
                it.first.services.forEach { service ->
                    try {
                        val dbMeta = resourceRepository.findByIdOrNull(service.resourceURI)
                        validateSourceUrl(service.resourceURI, harvestSource, dbMeta)
                        val result =
                            upsertResource(
                                uri = service.resourceURI,
                                type = ResourceType.DATASERVICE,
                                harvestedChecksum = computeChecksum(service.harvestedService),
                                harvestDate = harvestDate,
                                forceUpdate = forceUpdate,
                                harvestSource = harvestSource,
                                dbMeta = dbMeta,
                            )
                        result?.let { serviceMeta ->
                            updatedServices.add(FdkIdAndUri(fdkId = serviceMeta.fdkId, uri = serviceMeta.uri))
                            val catalogRecordModel =
                                createCatalogRecordModel(
                                    resourceUri = serviceMeta.uri,
                                    fdkId = serviceMeta.fdkId,
                                    parentFdkUri = catalogFdkUri,
                                    issued = serviceMeta.issued,
                                    modified = serviceMeta.modified,
                                    fdkUriBase = applicationProperties.dataserviceUri,
                                )
                            val graphWithRecords = service.harvestedService.union(catalogRecordModel)
                            val graphString = graphWithRecords.createRDFResponse(Lang.TURTLE)
                            resourceGraphs[serviceMeta.fdkId] = graphString
                        }
                    } catch (conflictError: HarvestSourceConflictException) {
                        logger.warn("Data service skipped due to conflict when harvesting {}: {}", harvestSource.uri, conflictError.message)
                    }
                }
            }

        val currentServiceUris =
            extractCatalogs(harvested, sourceUrl)
                .flatMap { it.services.map { s -> s.resourceURI } }
                .toSet()
        removedServices.addAll(findRemovedResources(ResourceType.DATASERVICE, currentServiceUris, harvestSource))

        removedServices.map { it.copy(removed = true) }.run { resourceRepository.saveAll(this) }
        logger.debug("Harvest of $sourceUrl completed")

        val report =
            HarvestReportBuilder.createSuccessReport(
                dataType = dataType,
                sourceId = sourceId,
                sourceUrl = sourceUrl,
                harvestDate = harvestDate,
                changedCatalogs = updatedCatalogs.map { FdkIdAndUri(fdkId = it.fdkId, uri = it.uri) },
                changedResources = updatedServices,
                removedResources = removedServices.map { FdkIdAndUri(fdkId = it.fdkId, uri = it.uri) },
                runId = runId,
            )

        if (updatedServices.isNotEmpty()) {
            resourceEventProducer.publishHarvestedEvents(
                dataType = HarvestDataType.dataservice,
                resources = updatedServices,
                resourceGraphs = resourceGraphs,
                runId = runId,
            )
        }

        if (removedServices.isNotEmpty()) {
            resourceEventProducer.publishRemovedEvents(
                dataType = HarvestDataType.dataservice,
                resources = removedServices.map { FdkIdAndUri(fdkId = it.fdkId, uri = it.uri) },
                runId = runId,
            )
        }

        return report
    }

    private fun extractCatalogs(
        harvested: Model,
        sourceURL: String,
    ): List<CatalogAndDataServiceModels> =
        harvested
            .listResourcesWithProperty(RDF.type, DCAT.Catalog)
            .toList()
            .excludeBlankNodes(sourceURL)
            .map { catalogResource ->
                val catalogServices: List<DataServiceModel> =
                    catalogResource
                        .listProperties(DCAT.service)
                        .toList()
                        .filter { it.isResourceProperty() }
                        .map { it.resource }
                        .excludeBlankNodes(sourceURL)
                        .map { it.extractDataService() }

                val catalogModelWithoutServices =
                    catalogResource
                        .extractCatalogModel()
                        .recursiveBlankNodeSkolem(catalogResource.uri)

                val servicesUnion = ModelFactory.createDefaultModel()
                catalogServices.forEach { servicesUnion.add(it.harvestedService) }

                CatalogAndDataServiceModels(
                    resourceURI = catalogResource.uri,
                    harvestedCatalog = catalogModelWithoutServices.union(servicesUnion),
                    harvestedCatalogWithoutServices = catalogModelWithoutServices,
                    services = catalogServices,
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

        listProperties()
            .toList()
            .filter { it.isResourceProperty() }
            .forEach { serviceModel.recursiveAddNonDataServiceResource(it.resource) }

        return DataServiceModel(
            resourceURI = uri,
            harvestedService = serviceModel.recursiveBlankNodeSkolem(uri),
        )
    }

    private fun List<Resource>.excludeBlankNodes(sourceURL: String): List<Resource> =
        filter {
            if (it.isURIResource) {
                true
            } else {
                logger.warn("Blank node catalog or data service filtered when harvesting $sourceURL")
                false
            }
        }

    private fun Model.addCatalogProperties(property: Statement): Model =
        when {
            property.predicate != DCAT.service && property.isResourceProperty() -> {
                add(property).recursiveAddNonDataServiceResource(property.resource)
            }

            property.predicate != DCAT.service -> {
                add(property)
            }

            property.isResourceProperty() && property.resource.isURIResource -> {
                add(property)
            }

            else -> {
                this
            }
        }

    private fun Model.recursiveAddNonDataServiceResource(resource: Resource): Model {
        if (resourceShouldBeAdded(resource)) {
            add(resource.listProperties())

            resource
                .listProperties()
                .toList()
                .filter { it.isResourceProperty() }
                .forEach { recursiveAddNonDataServiceResource(it.resource) }
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
            types.contains(DCAT.DataService) -> false
            !resource.isURIResource -> true
            containsTriple("<${resource.uri}>", "a", "?o") -> false
            else -> true
        }
    }

    private data class CatalogAndDataServiceModels(
        val resourceURI: String,
        val harvestedCatalog: Model,
        val harvestedCatalogWithoutServices: Model,
        val services: List<DataServiceModel>,
    )

    private data class DataServiceModel(
        val resourceURI: String,
        val harvestedService: Model,
    )
}
