package no.fdk.fdk_harvester.harvester

import no.fdk.fdk_harvester.config.ApplicationProperties
import no.fdk.fdk_harvester.kafka.ResourceEventProducer
import no.fdk.fdk_harvester.model.*
import no.fdk.fdk_harvester.model.ResourceEntity
import no.fdk.fdk_harvester.rdf.*
import no.fdk.fdk_harvester.rdf.createInformationModelCatalogRecordModel
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
import org.apache.jena.vocabulary.SKOS
import org.springframework.data.repository.findByIdOrNull
import org.springframework.stereotype.Service
import java.util.*

/** Harvests information model catalogs from RDF and publishes informationmodel events (with FDK catalog records in the graph). */
@Service
class InformationModelHarvester(
    private val applicationProperties: ApplicationProperties,
    private val resourceRepository: ResourceRepository,
    harvestSourceRepository: HarvestSourceRepository,
    private val resourceEventProducer: ResourceEventProducer
) : BaseHarvester(harvestSourceRepository) {

    fun harvestInformationModelCatalog(source: HarvestDataSource, harvestDate: Calendar, forceUpdate: Boolean, runId: String): HarvestReport? =
        validateAndHarvest(source, harvestDate, forceUpdate, runId, "informationmodel", requiresAcceptHeader = true)

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
        val updatedModels = mutableListOf<ResourceEntity>()
        val removedModels = mutableListOf<ResourceEntity>()
        val resourceGraphs = mutableMapOf<String, String>()
        val catalogPairs = splitCatalogsFromRDF(harvested, sourceUrl)
            .map { Pair(it, resourceRepository.findByIdOrNull(it.resourceURI)) }
        // Validate source ownership for all catalogs and models before filtering by change (avoids reporting 0 change when feed contains resources owned by another source)
        catalogPairs.forEach { (catalog, _) ->
            validateSourceUrl(catalog.resourceURI, harvestSource, resourceRepository.findByIdOrNull(catalog.resourceURI))
            catalog.models.forEach { infoModel ->
                validateSourceUrl(infoModel.resourceURI, harvestSource, resourceRepository.findByIdOrNull(infoModel.resourceURI))
            }
        }
        catalogPairs
            .filter { forceUpdate || it.first.catalogHasChanges(it.second) }
            .forEach {
                val dbMeta = it.second
                validateSourceUrl(it.first.resourceURI, harvestSource, dbMeta)
                val catalogChecksum = computeChecksum(it.first.harvestedCatalog)
                val updatedCatalogMeta = if (dbMeta == null || it.first.catalogHasChanges(dbMeta)) {
                    it.first.mapToResource(harvestDate, dbMeta, catalogChecksum, harvestSource)
                        .also { resourceRepository.save(it) }
                } else {
                    if (forceUpdate) {
                        dbMeta.copy(checksum = catalogChecksum, modified = harvestDate.toInstant(), harvestSource = harvestSource)
                            .also { resourceRepository.save(it) }
                    } else {
                        dbMeta
                    }
                }
                updatedCatalogs.add(updatedCatalogMeta)

                val catalogFdkUri = "${applicationProperties.informationmodelUri.substringBeforeLast("/")}/catalogs/${updatedCatalogMeta.fdkId}"
                it.first.models.forEach { infoModel ->
                    validateSourceUrl(infoModel.resourceURI, harvestSource, resourceRepository.findByIdOrNull(infoModel.resourceURI))
                    val result = infoModel.updateDBOs(harvestDate, forceUpdate, harvestSource)
                    result?.let { modelMeta ->
                        updatedModels.add(modelMeta)
                        val catalogRecordModel = createInformationModelCatalogRecordModel(
                            informationModelUri = modelMeta.uri,
                            informationModelFdkId = modelMeta.fdkId,
                            catalogFdkUri = catalogFdkUri,
                            issued = modelMeta.issued,
                            modified = modelMeta.modified,
                            informationModelUriBase = applicationProperties.informationmodelUri
                        )
                        val graphWithRecords = infoModel.harvested.union(catalogRecordModel)
                        val graphString = graphWithRecords.createRDFResponse(Lang.TURTLE)
                        resourceGraphs[modelMeta.fdkId] = graphString
                    }
                }
            }
        
        // Mark models as removed if they were harvested from this source but are no longer present
        val modelsFromThisSource = resourceRepository.findAllByType(ResourceType.INFORMATIONMODEL)
            .filter { it.harvestSource.id == harvestSource.id && !it.removed }
        val currentModelUris = catalogPairs.flatMap { it.first.models.map { m -> m.resourceURI } }.toSet()
        removedModels.addAll(
            modelsFromThisSource.filter { !currentModelUris.contains(it.uri) }
        )
        removedModels.map { it.copy(removed = true) }.run { resourceRepository.saveAll(this) }

        logger.debug("Harvest of $sourceUrl completed")
        val report = HarvestReportBuilder.createSuccessReport(
            dataType = dataType,
            sourceId = sourceId,
            sourceUrl = sourceUrl,
            harvestDate = harvestDate,
            changedCatalogs = updatedCatalogs.map { FdkIdAndUri(fdkId = it.fdkId, uri = it.uri) },
            changedResources = updatedModels.map { FdkIdAndUri(fdkId = it.fdkId, uri = it.uri) },
            removedResources = removedModels.map { FdkIdAndUri(fdkId = it.fdkId, uri = it.uri) },
            runId = runId
        )

        if (updatedModels.isNotEmpty()) {
            resourceEventProducer.publishHarvestedEvents(
                dataType = HarvestDataType.informationmodel,
                resources = updatedModels.map { FdkIdAndUri(fdkId = it.fdkId, uri = it.uri) },
                resourceGraphs = resourceGraphs,
                runId = runId
            )
        }

        if (removedModels.isNotEmpty()) {
            resourceEventProducer.publishRemovedEvents(
                dataType = HarvestDataType.informationmodel,
                resources = removedModels.map { FdkIdAndUri(fdkId = it.fdkId, uri = it.uri) },
                runId = runId
            )
        }

        return report
    }

    private fun InformationModelRDFModel.updateDBOs(
        harvestDate: Calendar,
        forceUpdate: Boolean,
        harvestSource: HarvestSourceEntity
    ): ResourceEntity? {
        val dbMeta = resourceRepository.findByIdOrNull(resourceURI)
        val harvestedChecksum = computeChecksum(harvested)
        return when {
            dbMeta == null || dbMeta.removed || modelHasChanges(dbMeta) -> {
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

    private fun CatalogAndInfoModels.mapToResource(
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

    private fun InformationModelRDFModel.mapToResource(
        harvestDate: Calendar,
        dbMeta: ResourceEntity?,
        checksum: String,
        harvestSource: HarvestSourceEntity
    ): ResourceEntity {
        val fdkId = dbMeta?.fdkId ?: createIdFromString(resourceURI)
        val issued = dbMeta?.issued ?: harvestDate.toInstant()

        return ResourceEntity(
            uri = resourceURI,
            type = ResourceType.INFORMATIONMODEL,
            fdkId = fdkId,
            removed = false,
            issued = issued,
            modified = harvestDate.toInstant(),
            checksum = checksum,
            harvestSource = harvestSource
        )
    }

    private fun CatalogAndInfoModels.catalogHasChanges(dbMeta: ResourceEntity?): Boolean =
        if (dbMeta == null) true
        else {
            val harvestedChecksum = computeChecksum(harvestedCatalog)
            harvestedChecksum != dbMeta.checksum
        }

    private fun InformationModelRDFModel.modelHasChanges(dbMeta: ResourceEntity?): Boolean =
        if (dbMeta == null) true
        else {
            val harvestedChecksum = computeChecksum(harvested)
            harvestedChecksum != dbMeta.checksum
        }

    private fun splitCatalogsFromRDF(harvested: Model, sourceURL: String): List<CatalogAndInfoModels> =
        harvested.listResourcesWithProperty(RDF.type, DCAT.Catalog)
            .toList()
            .filterBlankNodeCatalogsAndModels(sourceURL)
            .map { catalogResource ->
                val catalogInfoModels: List<InformationModelRDFModel> = catalogResource.listProperties(ModellDCATAPNO.model)
                    .toList()
                    .map { it.resource }
                    .filterBlankNodeCatalogsAndModels(sourceURL)
                    .filter { catalogContainsInfoModel(harvested, catalogResource.uri, it.uri) }
                    .map { infoModel -> infoModel.extractInformationModel() }

                val catalogModelWithoutInfoModels = catalogResource.extractCatalogModel()
                    .recursiveBlankNodeSkolem(catalogResource.uri)

                val catalogModel = ModelFactory.createDefaultModel()
                catalogInfoModels.forEach { catalogModel.add(it.harvested) }

                CatalogAndInfoModels(
                    resourceURI = catalogResource.uri,
                    harvestedCatalog = catalogModel.union(catalogModelWithoutInfoModels),
                    harvestedCatalogWithoutInfoModels = catalogModelWithoutInfoModels,
                    models = catalogInfoModels
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

    private fun List<Resource>.filterBlankNodeCatalogsAndModels(sourceURL: String): List<Resource> =
        filter {
            if (it.isURIResource) true
            else {
                logger.error(
                    "Failed harvest of catalog or model for $sourceURL, unable to harvest blank node catalogs and models",
                    Exception("unable to harvest blank node catalogs and models")
                )
                false
            }
        }

    private fun Model.addCatalogProperties(property: Statement): Model =
        when {
            property.predicate != ModellDCATAPNO.model && property.isResourceProperty() ->
                add(property).recursiveAddNonInformationModelResource(property.resource)
            property.predicate != ModellDCATAPNO.model -> add(property)
            property.isResourceProperty() && property.resource.isURIResource -> add(property)
            else -> this
        }

    private fun Resource.extractInformationModel(): InformationModelRDFModel {
        val infoModel = listProperties().toModel()
        infoModel.setNsPrefixes(model.nsPrefixMap)

        listProperties().toList()
            .filter { it.isResourceProperty() }
            .forEach { infoModel.recursiveAddNonInformationModelResource(it.resource) }

        return InformationModelRDFModel(resourceURI = uri, harvested = infoModel.recursiveBlankNodeSkolem(uri))
    }

    private fun Model.addCodeElementsAssociatedWithCodeList(resource: Resource): Model {
        resource.model
            .listResourcesWithProperty(RDF.type, ModellDCATAPNO.CodeElement)
            .toList()
            .filter { it.hasProperty(SKOS.inScheme, resource) }
            .forEach { codeElement ->
                add(codeElement.listProperties())

                codeElement.listProperties().toList()
                    .filter { it.isResourceProperty() }
                    .forEach { add(it.resource.listProperties()) }
            }

        return this
    }

    private fun Model.recursiveAddNonInformationModelResource(resource: Resource): Model =
        if (resource.isURIResource && containsTriple("<${resource.uri}>", "a", "?o")) this
        else {
            val types = resource.listProperties(RDF.type)
                .toList()
                .map { it.`object` }

            if (!types.contains(ModellDCATAPNO.InformationModel)) {
                add(resource.listProperties())

                resource.listProperties().toList()
                    .filter { it.isResourceProperty() }
                    .forEach { recursiveAddNonInformationModelResource(it.resource) }

                if (types.contains(ModellDCATAPNO.CodeList)) addCodeElementsAssociatedWithCodeList(resource)
            }

            this
        }

    private fun catalogContainsInfoModel(model: Model, catalogURI: String, infoModelURI: String): Boolean =
        model.containsTriple("<$catalogURI>", "<${ModellDCATAPNO.model.uri}>", "<$infoModelURI>")
            && model.containsTriple("<$infoModelURI>", "a", "<${ModellDCATAPNO.InformationModel.uri}>")

    // Data classes from InformationModelHarvestHelpers
    private data class CatalogAndInfoModels(
        val resourceURI: String,
        val harvestedCatalog: Model,
        val harvestedCatalogWithoutInfoModels: Model,
        val models: List<InformationModelRDFModel>,
    )

    private data class InformationModelRDFModel(
        val resourceURI: String,
        val harvested: Model
    )
}
