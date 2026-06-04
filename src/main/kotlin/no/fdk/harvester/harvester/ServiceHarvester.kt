package no.fdk.harvester.harvester

import no.fdk.harvester.adapter.DefaultOrganizationsAdapter
import no.fdk.harvester.config.ApplicationProperties
import no.fdk.harvester.kafka.ResourceEventProducer
import no.fdk.harvester.model.HarvestDataSource
import no.fdk.harvester.model.HarvestReport
import no.fdk.harvester.model.Organization
import no.fdk.harvester.rdf.CPSV
import no.fdk.harvester.rdf.CPSVNO
import no.fdk.harvester.rdf.CV
import no.fdk.harvester.rdf.DCATNO
import no.fdk.harvester.repository.HarvestSourceRepository
import no.fdk.harvester.repository.ResourceRepository
import org.apache.jena.rdf.model.Model
import org.apache.jena.rdf.model.Property
import org.apache.jena.rdf.model.RDFNode
import org.apache.jena.rdf.model.Resource
import org.apache.jena.vocabulary.DCAT
import org.apache.jena.vocabulary.DCTerms
import org.apache.jena.vocabulary.RDF
import org.springframework.stereotype.Service
import java.util.Calendar
import no.fdk.harvest.DataType as HarvestDataType

/** Harvests CPSV/DCAT service catalogs from RDF and publishes service events (with FDK catalog records in the graph). */
@Service
class ServiceHarvester(
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
    fun harvestServices(
        source: HarvestDataSource,
        harvestDate: Calendar,
        forceUpdate: Boolean,
        runId: String,
    ): HarvestReport? = validateAndHarvest(source, harvestDate, forceUpdate, runId, "service", requiresAcceptHeader = true)

    override val harvestConfig =
        ResourceHarvestConfig(
            harvestDataType = HarvestDataType.service,
            resourceType = no.fdk.harvester.model.ResourceType.SERVICE,
            containerResourceType = no.fdk.harvester.model.ResourceType.CATALOG,
            fdkResourceUriBase = applicationProperties.serviceUri,
            containerFdkUriBase = "${applicationProperties.serviceUri.substringBeforeLast("/")}/catalogs",
            generatedCatalogNbLabel = "Tjenestekatalog",
            generatedCatalogEnLabel = "Service catalog",
            conflictSkipLabel = "Service",
            missingParentLogMessage = { uri -> "The service $uri is missing associated catalog uri" },
        )

    override fun containerRdfType(): Resource = DCAT.Catalog

    override fun memberLinkProperty(): Property = DCATNO.containsService

    override fun listMembers(
        harvested: Model,
        sourceURL: String,
    ): List<MemberRDFModel> =
        harvested
            .listResourcesWithServiceType()
            .excludeBlankNodes(sourceURL)
            .map { it.extractMember(memberLinkProperty()) }

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
        )

    override fun isSeparatelyHarvestedMemberType(types: List<RDFNode>): Boolean =
        types.contains(CPSV.PublicService) ||
            types.contains(CPSVNO.Service) ||
            types.contains(CV.Event) ||
            types.contains(CV.BusinessEvent) ||
            types.contains(CV.LifeEvent)

    override fun postProcessMemberModel(
        model: Model,
        resource: Resource,
        types: List<RDFNode>,
    ) {
        if (types.contains(CV.Participation)) {
            model.addAgentsAssociatedWithParticipation(resource)
        }
    }

    private fun Model.listResourcesWithServiceType(): List<Resource> {
        val publicServices = listResourcesWithProperty(RDF.type, CPSV.PublicService).toList()
        val cpsvnoServices = listResourcesWithProperty(RDF.type, CPSVNO.Service).toList()
        return publicServices + cpsvnoServices
    }

    private fun Model.addAgentsAssociatedWithParticipation(resource: Resource): Model {
        resource.model
            .listResourcesWithProperty(RDF.type, DCTerms.Agent)
            .toList()
            .filter { it.hasProperty(CV.playsRole, resource) }
            .forEach { codeElement ->
                add(codeElement.listProperties())
                codeElement
                    .listProperties()
                    .toList()
                    .filter { it.isResourceProperty() }
                    .forEach { add(it.resource.listProperties()) }
            }
        return this
    }
}
