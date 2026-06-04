package no.fdk.harvester.harvester

import no.fdk.harvester.adapter.DefaultOrganizationsAdapter
import no.fdk.harvester.config.ApplicationProperties
import no.fdk.harvester.kafka.ResourceEventProducer
import no.fdk.harvester.model.HarvestDataSource
import no.fdk.harvester.model.HarvestReport
import no.fdk.harvester.model.Organization
import no.fdk.harvester.model.ResourceType
import no.fdk.harvester.rdf.DCAT3
import no.fdk.harvester.repository.HarvestSourceRepository
import no.fdk.harvester.repository.ResourceRepository
import org.apache.jena.rdf.model.Model
import org.apache.jena.rdf.model.Property
import org.apache.jena.rdf.model.RDFNode
import org.apache.jena.rdf.model.Resource
import org.apache.jena.vocabulary.DCAT
import org.apache.jena.vocabulary.RDF
import org.springframework.stereotype.Service
import java.util.Calendar
import no.fdk.harvest.DataType as HarvestDataType

/** Harvests DCAT dataset catalogs from RDF and publishes dataset events (with FDK catalog records in the graph). */
@Service
class DatasetHarvester(
    applicationProperties: ApplicationProperties,
    orgAdapter: DefaultOrganizationsAdapter,
    resourceRepository: ResourceRepository,
    harvestSourceRepository: HarvestSourceRepository,
    resourceEventProducer: ResourceEventProducer,
) : ResourceHarvester(
        harvestSourceRepository,
        resourceRepository,
        applicationProperties,
        orgAdapter,
        resourceEventProducer,
    ) {
    fun harvestDatasetCatalog(
        source: HarvestDataSource,
        harvestDate: Calendar,
        forceUpdate: Boolean,
        runId: String,
    ): HarvestReport? = validateAndHarvest(source, harvestDate, forceUpdate, runId, "dataset", requiresAcceptHeader = true)

    override val harvestConfig =
        ResourceHarvestConfig(
            harvestDataType = HarvestDataType.dataset,
            resourceType = ResourceType.DATASET,
            containerResourceType = ResourceType.CATALOG,
            fdkResourceUriBase = applicationProperties.datasetUri,
            containerFdkUriBase = "${applicationProperties.datasetUri.substringBeforeLast("/")}/catalogs",
            generatedCatalogNbLabel = "Datasettkatalog",
            generatedCatalogEnLabel = "Dataset catalog",
            conflictSkipLabel = "Dataset",
            missingParentLogMessage = { uri -> "The dataset $uri is missing associated catalog uri" },
        )

    override fun containerRdfType(): Resource = DCAT.Catalog

    override fun memberLinkProperty(): Property = DCAT.dataset

    override fun listMembers(
        harvested: Model,
        sourceURL: String,
    ): List<MemberRDFModel> {
        val datasets = harvested.listResourcesWithProperty(RDF.type, DCAT.Dataset).toList()
        val series = harvested.listResourcesWithProperty(RDF.type, DCAT3.DatasetSeries).toList()
        return (datasets + series)
            .distinct()
            .excludeBlankNodes(sourceURL)
            .map { it.extractMember(memberLinkProperty()) }
    }

    override fun extractContainers(
        harvested: Model,
        members: List<MemberRDFModel>,
        sourceURL: String,
        organization: Organization?,
    ): List<ContainerRDFModel> =
        extractContainersWithOrphans(
            harvested = harvested,
            members = members,
            sourceURL = sourceURL,
            organization = organization,
            resolveContainerMemberUris = { containerResource ->
                containerResource
                    .listProperties(memberLinkProperty())
                    .toList()
                    .filter { it.isResourceProperty() }
                    .map { it.resource }
                    .flatMap { it.expandDatasetSeriesMembers() }
                    .filter { it.isHarvestableDataset() }
                    .excludeBlankNodes(sourceURL)
                    .map { it.uri }
                    .toSet()
            },
        )

    override fun isSeparatelyHarvestedMemberType(types: List<RDFNode>): Boolean =
        types.contains(DCAT.Dataset) || types.contains(DCAT3.DatasetSeries)

    private fun Resource.expandDatasetSeriesMembers(): List<Resource> {
        val types = listProperties(RDF.type).toList().map { it.`object` }
        return if (types.contains(DCAT3.DatasetSeries)) {
            val datasetsInSeries = model.listResourcesWithProperty(DCAT3.inSeries, this).toList()
            datasetsInSeries + this
        } else {
            listOf(this)
        }
    }

    private fun Resource.isHarvestableDataset(): Boolean {
        val types = listProperties(RDF.type).toList().map { it.`object` }
        return types.contains(DCAT.Dataset) || types.contains(DCAT3.DatasetSeries)
    }
}
