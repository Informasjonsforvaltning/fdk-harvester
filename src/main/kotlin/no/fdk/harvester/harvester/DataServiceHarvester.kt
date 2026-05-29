package no.fdk.harvester.harvester

import no.fdk.harvester.adapter.DefaultOrganizationsAdapter
import no.fdk.harvester.config.ApplicationProperties
import no.fdk.harvester.kafka.ResourceEventProducer
import no.fdk.harvester.model.HarvestDataSource
import no.fdk.harvester.model.HarvestReport
import no.fdk.harvester.model.Organization
import no.fdk.harvester.model.ResourceType
import no.fdk.harvester.repository.HarvestSourceRepository
import no.fdk.harvester.repository.ResourceRepository
import org.apache.jena.rdf.model.Model
import org.apache.jena.rdf.model.RDFNode
import org.apache.jena.rdf.model.Resource
import org.apache.jena.vocabulary.DCAT
import org.apache.jena.vocabulary.RDF
import org.springframework.stereotype.Service
import java.util.Calendar
import no.fdk.harvest.DataType as HarvestDataType

/** Harvests DCAT data service catalogs from RDF and publishes dataservice events (with FDK catalog records in the graph). */
@Service
class DataServiceHarvester(
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
    fun harvestDataServiceCatalog(
        source: HarvestDataSource,
        harvestDate: Calendar,
        forceUpdate: Boolean,
        runId: String,
    ): HarvestReport? = validateAndHarvest(source, harvestDate, forceUpdate, runId, "dataservice", requiresAcceptHeader = true)

    override val harvestConfig =
        ResourceHarvestConfig(
            harvestDataType = HarvestDataType.dataservice,
            resourceType = ResourceType.DATASERVICE,
            containerResourceType = ResourceType.CATALOG,
            fdkResourceUriBase = applicationProperties.dataserviceUri,
            containerFdkUriBase = "${applicationProperties.dataserviceUri.substringBeforeLast("/")}/catalogs",
            generatedCatalogNbLabel = "Datatjenestekatalog",
            generatedCatalogEnLabel = "Data service catalog",
            conflictSkipLabel = "Data service",
            missingParentLogMessage = { uri -> "The data service $uri is missing associated catalog uri" },
        )

    override fun containerRdfType(): Resource = DCAT.Catalog

    override fun listMembers(
        harvested: Model,
        sourceURL: String,
    ): List<MemberRDFModel> =
        harvested
            .listResourcesWithProperty(RDF.type, DCAT.DataService)
            .toList()
            .excludeBlankNodes(sourceURL)
            .map { it.extractMember(DCAT.service) }

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
            memberLinkProperty = DCAT.service,
            addMembersToGeneratedContainer = { memberUris ->
                memberUris.forEach { addProperty(DCAT.service, model.createResource(it)) }
                this
            },
        )

    override fun isSeparatelyHarvestedMemberType(types: List<RDFNode>): Boolean = types.contains(DCAT.DataService)
}
