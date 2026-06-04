package no.fdk.harvester.harvester

import no.fdk.harvester.adapter.DefaultOrganizationsAdapter
import no.fdk.harvester.config.ApplicationProperties
import no.fdk.harvester.kafka.ResourceEventProducer
import no.fdk.harvester.model.HarvestDataSource
import no.fdk.harvester.model.HarvestReport
import no.fdk.harvester.model.Organization
import no.fdk.harvester.model.ResourceType
import no.fdk.harvester.rdf.DCATNO
import no.fdk.harvester.rdf.ModellDCATAPNO
import no.fdk.harvester.repository.HarvestSourceRepository
import no.fdk.harvester.repository.ResourceRepository
import org.apache.jena.rdf.model.Model
import org.apache.jena.rdf.model.Property
import org.apache.jena.rdf.model.RDFNode
import org.apache.jena.rdf.model.Resource
import org.apache.jena.vocabulary.DCAT
import org.apache.jena.vocabulary.RDF
import org.apache.jena.vocabulary.SKOS
import org.springframework.stereotype.Service
import java.util.Calendar
import no.fdk.harvest.DataType as HarvestDataType

/** Harvests information model catalogs from RDF and publishes informationmodel events (with FDK catalog records in the graph). */
@Service
class InformationModelHarvester(
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
    fun harvestInformationModelCatalog(
        source: HarvestDataSource,
        harvestDate: Calendar,
        forceUpdate: Boolean,
        runId: String,
    ): HarvestReport? = validateAndHarvest(source, harvestDate, forceUpdate, runId, "informationmodel", requiresAcceptHeader = true)

    override val harvestConfig =
        ResourceHarvestConfig(
            harvestDataType = HarvestDataType.informationmodel,
            resourceType = ResourceType.INFORMATIONMODEL,
            containerResourceType = ResourceType.CATALOG,
            fdkResourceUriBase = applicationProperties.informationmodelUri,
            containerFdkUriBase = "${applicationProperties.informationmodelUri.substringBeforeLast("/")}/catalogs",
            generatedCatalogNbLabel = "Informasjonsmodellkatalog",
            generatedCatalogEnLabel = "Information model catalog",
            conflictSkipLabel = "Information model",
            missingParentLogMessage = { uri -> "The information model $uri is missing associated catalog uri" },
        )

    override fun containerRdfType(): Resource = DCAT.Catalog

    override fun memberLinkProperty(): Property = ModellDCATAPNO.InformationModel

    override fun listMembers(
        harvested: Model,
        sourceURL: String,
    ): List<MemberRDFModel> =
        harvested
            .listResourcesWithProperty(RDF.type, ModellDCATAPNO.InformationModel)
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

    override fun isSeparatelyHarvestedMemberType(types: List<RDFNode>): Boolean = types.contains(ModellDCATAPNO.InformationModel)

    override fun postProcessMemberModel(
        model: Model,
        resource: Resource,
        types: List<RDFNode>,
    ) {
        if (types.contains(ModellDCATAPNO.CodeList)) {
            model.addCodeElementsAssociatedWithCodeList(resource)
        }
    }

    private fun Model.addCodeElementsAssociatedWithCodeList(resource: Resource): Model {
        resource.model
            .listResourcesWithProperty(RDF.type, ModellDCATAPNO.CodeElement)
            .toList()
            .filter { it.hasProperty(SKOS.inScheme, resource) }
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
