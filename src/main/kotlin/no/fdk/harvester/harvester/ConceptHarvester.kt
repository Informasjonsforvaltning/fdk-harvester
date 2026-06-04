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
import org.apache.jena.rdf.model.Property
import org.apache.jena.rdf.model.RDFNode
import org.apache.jena.rdf.model.Resource
import org.apache.jena.vocabulary.RDF
import org.apache.jena.vocabulary.SKOS
import org.springframework.stereotype.Service
import java.util.Calendar
import no.fdk.harvest.DataType as HarvestDataType

/** Harvests SKOS concept collections from RDF and publishes concept events (with FDK catalog records in the graph). */
@Service
class ConceptHarvester(
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
    fun harvestConceptCollection(
        source: HarvestDataSource,
        harvestDate: Calendar,
        forceUpdate: Boolean,
        runId: String,
    ): HarvestReport? = validateAndHarvest(source, harvestDate, forceUpdate, runId, "concept", requiresAcceptHeader = true)

    override val harvestConfig =
        ResourceHarvestConfig(
            harvestDataType = HarvestDataType.concept,
            resourceType = ResourceType.CONCEPT,
            containerResourceType = ResourceType.COLLECTION,
            fdkResourceUriBase = applicationProperties.conceptUri,
            containerFdkUriBase = "${applicationProperties.conceptUri.substringBeforeLast("/")}/collections",
            generatedCatalogNbLabel = "Begrepssamling",
            generatedCatalogEnLabel = "Concept collection",
            conflictSkipLabel = "Concept",
            missingParentLogMessage = { uri -> "The concept $uri is missing associated collection uri" },
            generatedContainerUriSuffix = "#GeneratedCollection",
        )

    override fun containerRdfType(): Resource = SKOS.Collection

    override fun memberLinkProperty(): Property = SKOS.member

    override fun listMembers(
        harvested: Model,
        sourceURL: String,
    ): List<MemberRDFModel> =
        harvested
            .listResourcesWithProperty(RDF.type, SKOS.Concept)
            .toList()
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

    override fun isSeparatelyHarvestedMemberType(types: List<RDFNode>): Boolean = types.contains(SKOS.Concept)
}
