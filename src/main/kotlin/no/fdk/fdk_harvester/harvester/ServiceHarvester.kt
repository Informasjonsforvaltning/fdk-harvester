package no.fdk.fdk_harvester.harvester

import no.fdk.fdk_harvester.adapter.DefaultOrganizationsAdapter
import no.fdk.fdk_harvester.config.ApplicationProperties
import no.fdk.fdk_harvester.kafka.ResourceEventProducer
import no.fdk.fdk_harvester.model.*
import no.fdk.fdk_harvester.rdf.*
import no.fdk.fdk_harvester.rdf.createServiceCatalogRecordModel
import no.fdk.fdk_harvester.repository.HarvestSourceRepository
import no.fdk.fdk_harvester.repository.ResourceRepository
import no.fdk.fdk_harvester.rdf.computeChecksum
import no.fdk.fdk_harvester.model.Organization
import no.fdk.harvest.DataType as HarvestDataType
import org.apache.jena.query.QueryExecutionFactory
import org.apache.jena.query.QueryFactory
import org.apache.jena.rdf.model.Model
import org.apache.jena.rdf.model.ModelFactory
import org.apache.jena.rdf.model.RDFNode
import org.apache.jena.rdf.model.Resource
import org.apache.jena.rdf.model.ResourceFactory
import org.apache.jena.rdf.model.Statement
import org.apache.jena.riot.Lang
import org.apache.jena.vocabulary.*
import org.springframework.data.repository.findByIdOrNull
import org.springframework.stereotype.Service
import java.util.*

/** Harvests CPSV/DCAT service catalogs from RDF and publishes service events (with FDK catalog records in the graph). */
@Service
class ServiceHarvester(
    private val applicationProperties: ApplicationProperties,
    private val orgAdapter: DefaultOrganizationsAdapter,
    private val resourceRepository: ResourceRepository,
    harvestSourceRepository: HarvestSourceRepository,
    private val resourceEventProducer: ResourceEventProducer
) : BaseHarvester(harvestSourceRepository) {

    fun harvestServices(source: HarvestDataSource, harvestDate: Calendar, forceUpdate: Boolean, runId: String): HarvestReport? =
        validateAndHarvest(source, harvestDate, forceUpdate, runId, "service", requiresAcceptHeader = true)

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
        val publisherId = source.publisherId
        val allServices = splitServicesFromRDF(harvested, sourceUrl)
        val organization = if (publisherId != null && allServices.containsFreeServices()) {
            orgAdapter.getOrganization(publisherId)
        } else null

        val catalogs = splitCatalogsFromRDF(harvested, allServices, sourceUrl, organization)
        val (updatedCatalogs, serviceUriToCatalogFdkUri) = updateCatalogs(catalogs, harvestDate, forceUpdate, harvestSource)
        val (updatedServices, resourceGraphs) = updateServices(allServices, harvestDate, forceUpdate, harvestSource, serviceUriToCatalogFdkUri)

        val removedServices = getServicesRemovedThisHarvest(
            allServices.map { it.resourceURI },
            harvestSource
        )
        removedServices.map { it.copy(removed = true) }
            .run { resourceRepository.saveAll(this) }

        val report = HarvestReportBuilder.createSuccessReport(
            dataType = dataType,
            sourceId = sourceId,
            sourceUrl = sourceUrl,
            harvestDate = harvestDate,
            changedCatalogs = updatedCatalogs,
            changedResources = updatedServices,
            removedResources = removedServices.map { FdkIdAndUri(fdkId = it.fdkId, uri = it.uri) },
            runId = runId
        )

        if (updatedServices.isNotEmpty()) {
            resourceEventProducer.publishHarvestedEvents(
                dataType = HarvestDataType.service,
                resources = updatedServices,
                resourceGraphs = resourceGraphs,
                runId = runId
            )
        }

        if (removedServices.isNotEmpty()) {
            resourceEventProducer.publishRemovedEvents(
                dataType = HarvestDataType.service,
                resources = removedServices.map { FdkIdAndUri(fdkId = it.fdkId, uri = it.uri) },
                runId = runId
            )
        }

        return report
    }

    private fun updateCatalogs(catalogs: List<ServiceCatalogRDFModel>, harvestDate: Calendar, forceUpdate: Boolean, harvestSource: HarvestSourceEntity): Pair<List<FdkIdAndUri>, Map<String, String>> {
        val serviceUriToCatalogFdkUri = mutableMapOf<String, String>()
        // Validate source ownership for all catalogs and services before filtering by change (avoids reporting 0 change when feed contains resources owned by another source)
        catalogs.forEach { catalog ->
            validateSourceUrl(catalog.resourceURI, harvestSource, resourceRepository.findByIdOrNull(catalog.resourceURI))
            catalog.services.forEach { serviceURI ->
                validateSourceUrl(serviceURI, harvestSource, resourceRepository.findByIdOrNull(serviceURI))
            }
        }
        val updatedCatalogs = catalogs
            .map { Pair(it, resourceRepository.findByIdOrNull(it.resourceURI)) }
            .filter { forceUpdate || it.first.hasChanges(it.second, computeChecksum(it.first.harvested)) }
            .map {
                val dbMeta = it.second
                validateSourceUrl(it.first.resourceURI, harvestSource, dbMeta)
                val catalogChecksum = computeChecksum(it.first.harvested)
                val updatedMeta = if (dbMeta == null || it.first.hasChanges(dbMeta, catalogChecksum)) {
                    it.first.mapToResourceMeta(harvestDate, dbMeta, catalogChecksum, harvestSource)
                        .also { resourceRepository.save(it) }
                } else {
                    if (forceUpdate) {
                        dbMeta.copy(checksum = catalogChecksum, modified = harvestDate.toInstant(), harvestSource = harvestSource)
                            .also { resourceRepository.save(it) }
                    } else {
                        dbMeta
                    }
                }

                val catalogFdkUri = "${applicationProperties.serviceUri.substringBeforeLast("/")}/catalogs/${updatedMeta.fdkId}"
                it.first.services.forEach { serviceURI: String ->
                    validateSourceUrl(serviceURI, harvestSource, resourceRepository.findByIdOrNull(serviceURI))
                    addIsPartOfToService(serviceURI, updatedMeta.uri)
                    serviceUriToCatalogFdkUri[serviceURI] = catalogFdkUri
                }

                FdkIdAndUri(fdkId = updatedMeta.fdkId, uri = updatedMeta.uri)
            }
        return Pair(updatedCatalogs, serviceUriToCatalogFdkUri)
    }

    private fun ServiceCatalogRDFModel.mapToResourceMeta(
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

    private fun updateServices(
        services: List<ServiceRDFModel>,
        harvestDate: Calendar,
        forceUpdate: Boolean,
        harvestSource: HarvestSourceEntity,
        serviceUriToCatalogFdkUri: Map<String, String>
    ): Pair<List<FdkIdAndUri>, Map<String, String>> {
        val resourceGraphs = mutableMapOf<String, String>()
        val updatedServices = services.mapNotNull {
            it.updateDBOs(harvestDate, forceUpdate, harvestSource)
                ?.let { meta ->
                    val catalogFdkUri = serviceUriToCatalogFdkUri[it.resourceURI]
                    val graphWithRecords = if (catalogFdkUri != null) {
                        val catalogRecordModel = createServiceCatalogRecordModel(
                            serviceUri = meta.uri,
                            serviceFdkId = meta.fdkId,
                            catalogFdkUri = catalogFdkUri,
                            issued = meta.issued,
                            modified = meta.modified,
                            serviceUriBase = applicationProperties.serviceUri
                        )
                        it.harvested.union(catalogRecordModel)
                    } else {
                        it.harvested
                    }
                    val graphString = graphWithRecords.createRDFResponse(Lang.TURTLE)
                    resourceGraphs[meta.fdkId] = graphString
                    FdkIdAndUri(fdkId = meta.fdkId, uri = it.resourceURI)
                }
        }
        return Pair(updatedServices, resourceGraphs)
    }

    private fun ServiceRDFModel.updateDBOs(harvestDate: Calendar, forceUpdate: Boolean, harvestSource: HarvestSourceEntity): ResourceEntity? {
        val dbMeta = resourceRepository.findByIdOrNull(resourceURI)
        validateSourceUrl(resourceURI, harvestSource, dbMeta)
        val harvestedChecksum = computeChecksum(harvested)
        return when {
            dbMeta == null || dbMeta.removed || hasChanges(dbMeta, harvestedChecksum) -> {
                val updatedMeta = mapToResourceMeta(harvestDate, dbMeta, harvestedChecksum, harvestSource)
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

    private fun ServiceRDFModel.mapToResourceMeta(
        harvestDate: Calendar,
        dbMeta: ResourceEntity?,
        checksum: String,
        harvestSource: HarvestSourceEntity
    ): ResourceEntity {
        val fdkId = dbMeta?.fdkId ?: createIdFromString(resourceURI)
        val issued = dbMeta?.issued ?: harvestDate.toInstant()

        return ResourceEntity(
            uri = resourceURI,
            type = ResourceType.SERVICE,
            fdkId = fdkId,
            removed = false,
            issued = issued,
            modified = harvestDate.toInstant(),
            checksum = checksum,
            harvestSource = harvestSource
        )
    }

    private fun getServicesRemovedThisHarvest(services: List<String>, harvestSource: HarvestSourceEntity): List<ResourceEntity> =
        resourceRepository.findAllByType(ResourceType.SERVICE)
            .filter { it.harvestSource.id == harvestSource.id && !it.removed && !services.contains(it.uri) }

    private fun addIsPartOfToService(serviceURI: String, catalogURI: String) {
        // Note: isPartOf relationship tracking removed - using harvestSource instead
        // This method kept for compatibility but does nothing
    }

    private fun ServiceRDFModel.hasChanges(dbMeta: ResourceEntity?, harvestedChecksum: String): Boolean =
        if (dbMeta == null) true
        else harvestedChecksum != dbMeta.checksum

    private fun ServiceCatalogRDFModel.hasChanges(dbMeta: ResourceEntity?, harvestedChecksum: String): Boolean =
        if (dbMeta == null) true
        else harvestedChecksum != dbMeta.checksum

    private fun splitCatalogsFromRDF(harvested: Model, allServices: List<ServiceRDFModel>,
                                     sourceURL: String, organization: Organization?): List<ServiceCatalogRDFModel> {
        val harvestedCatalogs = harvested.listResourcesWithProperty(RDF.type, DCAT.Catalog)
            .toList()
            .excludeBlankNodes(sourceURL)
            .map { resource ->
                val catalogServices: Set<String> = resource.listProperties(DCATNO.containsService)
                    .toList()
                    .filter { it.isResourceProperty() }
                    .map { it.resource }
                    .excludeBlankNodes(sourceURL)
                    .map { it.uri }
                    .toSet()

                val catalogModelWithoutServices = resource.extractCatalogModel()
                    .recursiveBlankNodeSkolem(resource.uri)

                val catalogModel = ModelFactory.createDefaultModel()
                allServices.filter { catalogServices.contains(it.resourceURI) }
                    .forEach { catalogModel.add(it.harvested) }

                ServiceCatalogRDFModel(
                    resourceURI = resource.uri,
                    harvestedWithoutServices = catalogModelWithoutServices,
                    harvested = catalogModel.union(catalogModelWithoutServices),
                    services = catalogServices
                )
            }

        return harvestedCatalogs.plus(generatedCatalog(
            allServices.filterNot { it.isMemberOfAnyCatalog },
            sourceURL,
            organization)
        )
    }

    private fun splitServicesFromRDF(harvested: Model, sourceURL: String): List<ServiceRDFModel> =
        harvested.listResourcesWithServiceType()
            .toList()
            .excludeBlankNodes(sourceURL)
            .map { serviceResource -> serviceResource.extractService() }

    private fun Resource.extractCatalogModel(): Model {
        val catalogModelWithoutServices = ModelFactory.createDefaultModel()
        catalogModelWithoutServices.setNsPrefixes(model.nsPrefixMap)

        listProperties()
            .toList()
            .forEach { catalogModelWithoutServices.addCatalogProperties(it) }

        return catalogModelWithoutServices
    }

    private fun Resource.extractService(): ServiceRDFModel {
        val serviceModel = listProperties().toModel()
        serviceModel.setNsPrefixes(model.nsPrefixMap)

        listProperties().toList()
            .filter { it.isResourceProperty() }
            .forEach { serviceModel.recursiveAddNonServiceResources(it.resource) }

        return ServiceRDFModel(
            resourceURI = uri,
            harvested = serviceModel.recursiveBlankNodeSkolem(uri),
            isMemberOfAnyCatalog = isMemberOfAnyCatalog()
        )
    }

    private fun Model.addCatalogProperties(property: Statement): Model =
        when {
            property.predicate != DCATNO.containsService && property.isResourceProperty() ->
                add(property).recursiveAddNonServiceResources(property.resource)
            property.predicate != DCATNO.containsService -> add(property)
            property.isResourceProperty() && property.resource.isURIResource -> add(property)
            else -> this
        }

    private fun Model.listResourcesWithServiceType(): List<Resource> {
        val publicServices = listResourcesWithProperty(RDF.type, CPSV.PublicService)
            .toList()

        val cpsvnoServices = listResourcesWithProperty(RDF.type, CPSVNO.Service)
            .toList()

        return listOf(publicServices, cpsvnoServices).flatten()
    }

    private fun List<Resource>.excludeBlankNodes(sourceURL: String): List<Resource> =
        filter {
            if (it.isURIResource) true
            else {
                logger.warn("Blank node service or catalog filtered when harvesting $sourceURL")
                false
            }
        }

    private fun Model.addAgentsAssociatedWithParticipation(resource: Resource): Model {
        resource.model
            .listResourcesWithProperty(RDF.type, DCTerms.Agent)
            .toList()
            .filter { it.hasProperty(CV.playsRole, resource) }
            .forEach { codeElement ->
                add(codeElement.listProperties())

                codeElement.listProperties().toList()
                    .filter { it.isResourceProperty() }
                    .forEach { add(it.resource.listProperties()) }
            }

        return this
    }

    private fun generatedCatalog(
        services: List<ServiceRDFModel>,
        sourceURL: String,
        organization: Organization?
    ): ServiceCatalogRDFModel {
        val serviceURIs = services.map { it.resourceURI }.toSet()
        val generatedCatalogURI = "$sourceURL#GeneratedCatalog"
        val catalogModelWithoutServices = createModelForHarvestSourceCatalog(generatedCatalogURI, serviceURIs, organization)

        val catalogModel = ModelFactory.createDefaultModel()
        services.forEach { catalogModel.add(it.harvested) }

        return ServiceCatalogRDFModel(
            resourceURI = generatedCatalogURI,
            harvestedWithoutServices = catalogModelWithoutServices,
            harvested = catalogModel.union(catalogModelWithoutServices),
            services = serviceURIs
        )
    }

    private fun createModelForHarvestSourceCatalog(
        catalogURI: String,
        services: Set<String>,
        organization: Organization?
    ): Model {
        val catalogModel = ModelFactory.createDefaultModel()
        catalogModel.createResource(catalogURI)
            .addProperty(RDF.type, DCAT.Catalog)
            .addPublisherForGeneratedCatalog(organization?.uri)
            .addLabelForGeneratedCatalog(organization)
            .addServicesForGeneratedCatalog(services)

        return catalogModel
    }

    private fun Resource.addPublisherForGeneratedCatalog(publisherURI: String?): Resource {
        if (publisherURI != null) {
            addProperty(
                DCTerms.publisher,
                ResourceFactory.createResource(publisherURI)
            )
        }

        return this
    }

    private fun Resource.addLabelForGeneratedCatalog(organization: Organization?): Resource {
        val nb: String? = organization?.prefLabel?.nb ?: organization?.name
        if (!nb.isNullOrBlank()) {
            val label = model.createLiteral("$nb - Tjenestekatalog", "nb")
            addProperty(RDFS.label, label)
        }

        val nn: String? = organization?.prefLabel?.nn ?: organization?.name
        if (!nn.isNullOrBlank()) {
            val label = model.createLiteral("$nn - Tjenestekatalog", "nn")
            addProperty(RDFS.label, label)
        }

        val en: String? = organization?.prefLabel?.en ?: organization?.name
        if (!en.isNullOrBlank()) {
            val label = model.createLiteral("$en - Service catalog", "en")
            addProperty(RDFS.label, label)
        }

        return this
    }

    private fun Resource.addServicesForGeneratedCatalog(services: Set<String>): Resource {
        services.forEach { addProperty(DCATNO.containsService, model.createResource(it)) }
        return this
    }

    private fun Model.recursiveAddNonServiceResources(resource: Resource): Model {
        val types = resource.listProperties(RDF.type)
            .toList()
            .map { it.`object` }

        if (resourceShouldBeAdded(resource, types)) {
            add(resource.listProperties())

            resource.listProperties().toList()
                .filter { it.isResourceProperty() }
                .forEach { recursiveAddNonServiceResources(it.resource) }
        }

        if (types.contains(CV.Participation)) addAgentsAssociatedWithParticipation(resource)

        return this
    }

    private fun Model.resourceShouldBeAdded(resource: Resource, types: List<RDFNode>): Boolean =
        when {
            types.contains(CPSV.PublicService) -> false
            types.contains(CPSVNO.Service) -> false
            types.contains(CV.Event) -> false
            types.contains(CV.BusinessEvent) -> false
            types.contains(CV.LifeEvent) -> false
            !resource.isURIResource -> true
            containsTriple("<${resource.uri}>", "a", "?o") -> false
            else -> true
        }

    private fun Resource.isMemberOfAnyCatalog(): Boolean {
        val askQuery = """ASK {
        ?catalog a <${DCAT.Catalog.uri}> .
        ?catalog <${DCATNO.containsService.uri}> <$uri> .
    }""".trimMargin()

        val query = QueryFactory.create(askQuery)
        return QueryExecutionFactory.create(query, model).execAsk()
    }

    // Data classes from ServiceHarvestHelpers
    private data class ServiceRDFModel(
        val resourceURI: String,
        val harvested: Model,
        val isMemberOfAnyCatalog: Boolean
    )

    private data class ServiceCatalogRDFModel(
        val resourceURI: String,
        val harvested: Model,
        val harvestedWithoutServices: Model,
        val services: Set<String>,
    )

    private fun List<ServiceRDFModel>.containsFreeServices(): Boolean =
        firstOrNull { !it.isMemberOfAnyCatalog } != null
}
