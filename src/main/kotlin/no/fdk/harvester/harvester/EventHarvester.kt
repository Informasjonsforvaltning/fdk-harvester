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
import org.apache.jena.vocabulary.RDF
import org.springframework.stereotype.Service
import java.util.Calendar
import no.fdk.harvest.DataType as HarvestDataType

/** Harvests event catalogs from RDF and publishes event events (with FDK catalog records in the graph). */
@Service
class EventHarvester(
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
    fun harvestEvents(
        source: HarvestDataSource,
        harvestDate: Calendar,
        forceUpdate: Boolean,
        runId: String,
    ): HarvestReport? = validateAndHarvest(source, harvestDate, forceUpdate, runId, "event", requiresAcceptHeader = true)

    override val harvestConfig =
        ResourceHarvestConfig(
            harvestDataType = HarvestDataType.event,
            resourceType = no.fdk.harvester.model.ResourceType.EVENT,
            containerResourceType = no.fdk.harvester.model.ResourceType.CATALOG,
            fdkResourceUriBase = applicationProperties.eventUri,
            containerFdkUriBase = "${applicationProperties.eventUri.substringBeforeLast("/")}/catalogs",
            generatedCatalogNbLabel = "Hendelseskatalog",
            generatedCatalogEnLabel = "Event catalog",
            conflictSkipLabel = "Event",
            missingParentLogMessage = { uri -> "The event $uri is missing associated catalog uri" },
        )

    override fun containerRdfType(): Resource = DCAT.Catalog

    override fun memberLinkProperty(): Property = DCATNO.containsEvent

    override fun listMembers(
        harvested: Model,
        sourceURL: String,
    ): List<MemberRDFModel> =
        harvested
            .listResourcesWithEventType()
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
        types.contains(CV.Event) ||
            types.contains(CV.BusinessEvent) ||
            types.contains(CV.LifeEvent) ||
            types.contains(CPSV.PublicService) ||
            types.contains(CPSVNO.Service)

    private fun Model.listResourcesWithEventType(): List<Resource> {
        val events = listResourcesWithProperty(RDF.type, CV.Event).toList()
        val businessEvents = listResourcesWithProperty(RDF.type, CV.BusinessEvent).toList()
        val lifeEvents = listResourcesWithProperty(RDF.type, CV.LifeEvent).toList()
        return events + businessEvents + lifeEvents
    }
}
