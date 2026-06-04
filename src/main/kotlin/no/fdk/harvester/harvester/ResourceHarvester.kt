package no.fdk.harvester.harvester

import no.fdk.harvester.adapter.DefaultOrganizationsAdapter
import no.fdk.harvester.config.ApplicationProperties
import no.fdk.harvester.kafka.ResourceEventProducer
import no.fdk.harvester.model.FdkIdAndUri
import no.fdk.harvester.model.HarvestDataSource
import no.fdk.harvester.model.HarvestReport
import no.fdk.harvester.model.HarvestSourceEntity
import no.fdk.harvester.model.Organization
import no.fdk.harvester.model.ResourceType
import no.fdk.harvester.rdf.computeChecksum
import no.fdk.harvester.rdf.containsTriple
import no.fdk.harvester.rdf.createCatalogRecordModel
import no.fdk.harvester.rdf.createIdFromString
import no.fdk.harvester.rdf.createRDFResponse
import no.fdk.harvester.repository.HarvestSourceRepository
import no.fdk.harvester.repository.ResourceRepository
import org.apache.jena.query.QueryExecutionFactory
import org.apache.jena.query.QueryFactory
import org.apache.jena.rdf.model.Model
import org.apache.jena.rdf.model.ModelFactory
import org.apache.jena.rdf.model.Property
import org.apache.jena.rdf.model.RDFNode
import org.apache.jena.rdf.model.Resource
import org.apache.jena.rdf.model.Statement
import org.apache.jena.riot.Lang
import org.apache.jena.vocabulary.RDF
import org.springframework.data.repository.findByIdOrNull
import java.util.Calendar
import no.fdk.harvest.DataType as HarvestDataType

/** A harvested member resource before catalog-record attachment. */
data class MemberRDFModel(
    val resourceURI: String,
    val harvested: Model,
    val isMemberOfAnyContainer: Boolean,
)

/** A DCAT catalog or SKOS collection and its member URIs extracted from the source RDF. */
data class ContainerRDFModel(
    val resourceURI: String,
    val harvested: Model,
    val harvestedWithoutMembers: Model,
    val memberURIs: Set<String>,
)

/**
 * Abstract base for every type-specific harvester. Extracts typed member resources and their
 * containers (DCAT catalogs / SKOS collections) from the source RDF, persists each member and each
 * container as a [no.fdk.harvester.model.ResourceEntity], and attaches an FDK catalog record linking
 * the member to its container. Members not linked from any container are gathered into a synthetic
 * container at `{sourceUrl}#GeneratedCatalog` / `#GeneratedCollection`.
 *
 * Containers are persisted with [ResourceHarvestConfig.containerResourceType] (CATALOG or
 * COLLECTION), the member's `dct:isPartOf` parent URI is built from the persisted container `fdkId`
 * (see [updateContainers]).
 *
 * Subclasses: [DatasetHarvester], [DataServiceHarvester], [InformationModelHarvester],
 * [ServiceHarvester], [EventHarvester], [ConceptHarvester].
 */
abstract class ResourceHarvester(
    harvestSourceRepository: HarvestSourceRepository,
    resourceRepository: ResourceRepository,
    protected val applicationProperties: ApplicationProperties,
    protected val orgAdapter: DefaultOrganizationsAdapter,
    protected val resourceEventProducer: ResourceEventProducer,
) : BaseHarvester(harvestSourceRepository, resourceRepository) {
    protected abstract val harvestConfig: ResourceHarvestConfig

    /** Per-type labels, URIs, and Kafka event ordering for resource harvesters. */
    data class ResourceHarvestConfig(
        val harvestDataType: HarvestDataType,
        val resourceType: ResourceType,
        val containerResourceType: ResourceType,
        val fdkResourceUriBase: String,
        val containerFdkUriBase: String,
        val generatedCatalogNbLabel: String,
        val generatedCatalogEnLabel: String,
        val conflictSkipLabel: String,
        val missingParentLogMessage: (resourceUri: String) -> String?,
        val generatedContainerUriSuffix: String = "#GeneratedCatalog",
    )

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
        val members = listMembers(harvested, sourceUrl)
        val organization = resolveOrganization(source, members)
        val containers = extractContainers(harvested, members, sourceUrl, organization)
        val (updatedContainers, memberToContainerFdkUri) =
            updateContainers(containers, harvestDate, forceUpdate, harvestSource)
        val (updatedMembers, resourceGraphs) =
            updateMembers(
                members,
                harvestDate,
                forceUpdate,
                harvestSource,
                memberToContainerFdkUri,
            )

        val removedMembers =
            findRemovedResources(
                harvestConfig.resourceType,
                members.map { it.resourceURI }.toSet(),
                harvestSource,
            )
        removedMembers
            .map { it.copy(removed = true) }
            .run { resourceRepository.saveAll(this) }

        val report =
            HarvestReportBuilder.createSuccessReport(
                dataType = dataType,
                sourceId = sourceId,
                sourceUrl = sourceUrl,
                harvestDate = harvestDate,
                changedCatalogs = updatedContainers,
                changedResources = updatedMembers,
                removedResources = removedMembers.map { FdkIdAndUri(fdkId = it.fdkId, uri = it.uri) },
                runId = runId,
            )

        if (updatedMembers.isNotEmpty()) {
            resourceEventProducer.publishHarvestedEvents(
                dataType = harvestConfig.harvestDataType,
                resources = updatedMembers,
                resourceGraphs = resourceGraphs,
                runId = runId,
                catalogGraphs = catalogGraphforUpdatedMember(updatedMembers, containers),
            )
        }

        if (removedMembers.isNotEmpty()) {
            resourceEventProducer.publishRemovedEvents(
                dataType = harvestConfig.harvestDataType,
                resources = removedMembers.map { FdkIdAndUri(fdkId = it.fdkId, uri = it.uri) },
                runId = runId,
            )
        }

        return report
    }

    protected open fun resolveOrganization(
        source: HarvestDataSource,
        members: List<MemberRDFModel>,
    ): Organization? {
        val publisherId = source.publisherId ?: return null
        return if (members.any { !it.isMemberOfAnyContainer }) {
            orgAdapter.getOrganization(publisherId)
        } else {
            null
        }
    }

    protected abstract fun listMembers(
        harvested: Model,
        sourceURL: String,
    ): List<MemberRDFModel>

    protected abstract fun extractContainers(
        harvested: Model,
        members: List<MemberRDFModel>,
        sourceURL: String,
        organization: Organization?,
    ): List<ContainerRDFModel>

    /**
     * Persists each container as a [ResourceHarvestConfig.containerResourceType]
     * [no.fdk.harvester.model.ResourceEntity] (CATALOG or COLLECTION) and resolves the
     * member-to-container FDK URI map used to attach `dct:isPartOf` records to members.
     *
     * A container is saved when it is new, was removed, its checksum changed, or [forceUpdate] is
     * true; its `fdkId` (derived from the container URI on first harvest and preserved thereafter) is
     * used to build the parent FDK URI for every member it links. Returns the containers that were
     * created or updated (for the report), unchanged containers are still resolved into the map but
     * excluded from the report.
     */
    protected fun updateContainers(
        containers: List<ContainerRDFModel>,
        harvestDate: Calendar,
        forceUpdate: Boolean,
        harvestSource: HarvestSourceEntity,
    ): Pair<List<FdkIdAndUri>, Map<String, String>> {
        val memberToContainerFdkUri = mutableMapOf<String, String>()
        val updatedContainers = mutableListOf<FdkIdAndUri>()

        containers.forEach { container ->
            val dbMeta = resourceRepository.findByIdOrNull(container.resourceURI)
            validateSourceUrl(container.resourceURI, harvestSource, dbMeta)

            val updated =
                upsertResource(
                    uri = container.resourceURI,
                    type = harvestConfig.containerResourceType,
                    harvestedChecksum = computeChecksum(container.harvested),
                    harvestDate = harvestDate,
                    forceUpdate = forceUpdate,
                    harvestSource = harvestSource,
                    dbMeta = dbMeta,
                )

            val fdkId = updated?.fdkId ?: dbMeta?.fdkId ?: createIdFromString(container.resourceURI)
            val containerFdkUri = "${harvestConfig.containerFdkUriBase}/$fdkId"
            container.memberURIs.forEach { memberUri ->
                memberToContainerFdkUri.putIfAbsent(memberUri, containerFdkUri)
            }
            if (updated != null) {
                updatedContainers.add(FdkIdAndUri(fdkId = fdkId, uri = container.resourceURI))
            }
        }

        return Pair(updatedContainers, memberToContainerFdkUri)
    }

    protected fun updateMembers(
        members: List<MemberRDFModel>,
        harvestDate: Calendar,
        forceUpdate: Boolean,
        harvestSource: HarvestSourceEntity,
        memberToContainerFdkUri: Map<String, String>,
    ): Pair<List<FdkIdAndUri>, Map<String, String>> {
        val resourceGraphs = mutableMapOf<String, String>()
        val updatedMembers =
            members.mapNotNull { member ->
                try {
                    val dbMeta = resourceRepository.findByIdOrNull(member.resourceURI)
                    validateSourceUrl(member.resourceURI, harvestSource, dbMeta)
                    upsertResource(
                        uri = member.resourceURI,
                        type = harvestConfig.resourceType,
                        harvestedChecksum = computeChecksum(member.harvested),
                        harvestDate = harvestDate,
                        forceUpdate = forceUpdate,
                        harvestSource = harvestSource,
                        dbMeta = dbMeta,
                    )?.let { meta ->
                        val parentFdkUri = memberToContainerFdkUri[member.resourceURI]
                        val catalogRecordModel =
                            createCatalogRecordModel(
                                resourceUri = meta.uri,
                                fdkId = meta.fdkId,
                                parentFdkUri = parentFdkUri,
                                issued = meta.issued,
                                modified = meta.modified,
                                fdkUriBase = harvestConfig.fdkResourceUriBase,
                                missingParentLogMessage =
                                    if (parentFdkUri == null) {
                                        harvestConfig.missingParentLogMessage(meta.uri)
                                    } else {
                                        null
                                    },
                            )
                        val graphWithRecords = member.harvested.union(catalogRecordModel)
                        val graphString = graphWithRecords.createRDFResponse(Lang.TURTLE)
                        resourceGraphs[meta.fdkId] = graphString
                        FdkIdAndUri(fdkId = meta.fdkId, uri = member.resourceURI)
                    }
                } catch (conflictError: HarvestSourceConflictException) {
                    logger.warn(
                        "{} skipped due to conflict when harvesting {}: {}",
                        harvestConfig.conflictSkipLabel,
                        harvestSource.uri,
                        conflictError.message,
                    )
                    null
                }
            }

        return Pair(updatedMembers, resourceGraphs)
    }

    /**
     * Extracts every [containerRdfType] container with its linked members, then appends a synthetic
     * container holding all orphan members (members not linked from any container).
     *
     * @param resolveContainerMemberUris resolves a container's member URIs; defaults to direct
     * [memberLinkProperty] links. Override to expand indirect members (e.g. dataset series).
     */
    protected fun extractContainersWithOrphans(
        harvested: Model,
        members: List<MemberRDFModel>,
        sourceURL: String,
        organization: Organization?,
        resolveContainerMemberUris: (containerResource: Resource) -> Set<String> = { containerResource ->
            containerResource
                .listProperties(memberLinkProperty())
                .toList()
                .filter { it.isResourceProperty() }
                .map { it.resource }
                .excludeBlankNodes(sourceURL)
                .map { it.uri }
                .toSet()
        },
    ): List<ContainerRDFModel> {
        val harvestedContainers =
            harvested
                .listResourcesWithProperty(RDF.type, containerRdfType())
                .toList()
                .excludeBlankNodes(sourceURL)
                .map { containerResource ->
                    val memberUris: Set<String> = resolveContainerMemberUris(containerResource)

                    val containerModelWithoutMembers =
                        containerResource
                            .extractContainerModel(memberLinkProperty())
                            .recursiveBlankNodeSkolem(containerResource.uri)

                    val containerModel = ModelFactory.createDefaultModel()
                    members
                        .filter { memberUris.contains(it.resourceURI) }
                        .forEach { containerModel.add(it.harvested) }

                    ContainerRDFModel(
                        resourceURI = containerResource.uri,
                        harvestedWithoutMembers = containerModelWithoutMembers,
                        harvested = containerModel.union(containerModelWithoutMembers),
                        memberURIs = memberUris,
                    )
                }

        return harvestedContainers.plus(
            buildGeneratedContainer(
                orphanMembers = members.filterNot { it.isMemberOfAnyContainer },
                sourceURL = sourceURL,
                organization = organization,
            ),
        )
    }

    protected fun buildGeneratedContainer(
        orphanMembers: List<MemberRDFModel>,
        sourceURL: String,
        organization: Organization?,
    ): ContainerRDFModel {
        val memberUris = orphanMembers.map { it.resourceURI }.toSet()
        val generatedContainerURI = "$sourceURL${harvestConfig.generatedContainerUriSuffix}"
        val catalogModelWithoutMembers =
            createModelForGeneratedContainer(
                containerURI = generatedContainerURI,
                memberUris = memberUris,
                organization = organization,
            )

        val catalogModel = ModelFactory.createDefaultModel()
        orphanMembers.forEach { catalogModel.add(it.harvested) }

        return ContainerRDFModel(
            resourceURI = generatedContainerURI,
            harvestedWithoutMembers = catalogModelWithoutMembers,
            harvested = catalogModel.union(catalogModelWithoutMembers),
            memberURIs = memberUris,
        )
    }

    protected fun createModelForGeneratedContainer(
        containerURI: String,
        memberUris: Set<String>,
        organization: Organization?,
    ): Model {
        val catalogModel = ModelFactory.createDefaultModel()
        catalogModel
            .createResource(containerURI)
            .addProperty(RDF.type, containerRdfType())
            .addPublisherForGeneratedCatalog(organization?.uri)
            .addLabelForGeneratedCatalog(
                organization,
                harvestConfig.generatedCatalogNbLabel,
                harvestConfig.generatedCatalogEnLabel,
            ).addMembersToContainer(memberUris)
        return catalogModel
    }

    protected fun Resource.extractContainerModel(memberLinkProperty: Property): Model {
        val containerModelWithoutMembers = ModelFactory.createDefaultModel()
        containerModelWithoutMembers.setNsPrefixes(model.nsPrefixMap)
        listProperties()
            .toList()
            .forEach { containerModelWithoutMembers.addContainerProperties(it, memberLinkProperty) }
        return containerModelWithoutMembers
    }

    protected open fun Resource.extractMember(memberLinkProperty: Property): MemberRDFModel {
        val memberModel = listProperties().toModel()
        memberModel.setNsPrefixes(model.nsPrefixMap)
        listProperties()
            .toList()
            .filter { it.isResourceProperty() }
            .forEach { memberModel.recursiveAddNonMemberResource(it.resource) }
        return MemberRDFModel(
            resourceURI = uri,
            harvested = memberModel.recursiveBlankNodeSkolem(uri),
            isMemberOfAnyContainer = isMemberOfContainer(memberLinkProperty),
        )
    }

    protected fun Model.addContainerProperties(
        property: Statement,
        memberLinkProperty: Property,
    ): Model =
        when {
            property.predicate != memberLinkProperty && property.isResourceProperty() -> {
                add(property).recursiveAddNonMemberResource(property.resource)
            }

            property.predicate != memberLinkProperty -> {
                add(property)
            }

            property.isResourceProperty() && property.resource.isURIResource -> {
                add(property)
            }

            else -> {
                this
            }
        }

    protected fun Model.recursiveAddNonMemberResource(resource: Resource): Model {
        val types = resource.listProperties(RDF.type).toList().map { it.`object` }
        if (shouldAddToTargetModel(resource, types)) {
            add(resource.listProperties())
            resource
                .listProperties()
                .toList()
                .filter { it.isResourceProperty() }
                .forEach { recursiveAddNonMemberResource(it.resource) }
        }
        postProcessMemberModel(this, resource, types)
        return this
    }

    private fun catalogGraphforUpdatedMember(
        updatedMembers: List<FdkIdAndUri>,
        containers: List<ContainerRDFModel>,
    ): Map<String, String> {
        val catalogGraphs = mutableMapOf<String, String>()

        updatedMembers.forEach { member ->
            val container = containers.firstOrNull { it.memberURIs.contains(member.uri) }
            val model = ModelFactory.createDefaultModel()
            if (container != null) {
                model.add(container.harvestedWithoutMembers)
                model.getResource(container.resourceURI).addMembersToContainer(setOf(member.uri))
            }

            catalogGraphs[member.fdkId] = model.createRDFResponse(Lang.TURTLE)
        }

        return catalogGraphs
    }

    /**
     * Whether [resource] should be added to this (the target) model. A resource is added unless it is
     * harvested as its own member ([isSeparatelyHarvestedMemberType]) or it is a URI resource already
     * present in the target model. The URI-presence check both de-duplicates and breaks cycles
     * (e.g. a resource referencing itself via dct:identifier).
     */
    private fun Model.shouldAddToTargetModel(
        resource: Resource,
        types: List<RDFNode>,
    ): Boolean =
        when {
            isSeparatelyHarvestedMemberType(types) -> false
            resource.isURIResource && containsTriple("<${resource.uri}>", "?p", "?o") -> false
            else -> true
        }

    protected open fun postProcessMemberModel(
        model: Model,
        resource: Resource,
        types: List<RDFNode>,
    ) {}

    /**
     * Whether [types] identifies a resource that is harvested as its own member (and therefore must
     * not be inlined into another member's graph). Resources of any other type are inlined.
     */
    protected abstract fun isSeparatelyHarvestedMemberType(types: List<RDFNode>): Boolean

    protected fun Resource.isMemberOfContainer(memberLinkProperty: Property): Boolean {
        val askQuery =
            """
            ASK {
                ?container a <${containerRdfType().uri}> .
                ?container <${memberLinkProperty.uri}> <$uri> .
            }
            """.trimIndent()
        val query = QueryFactory.create(askQuery)
        return QueryExecutionFactory.create(query, model).execAsk()
    }

    private fun Resource.addMembersToContainer(memberUris: Set<String>): Resource {
        memberUris.forEach { addProperty(memberLinkProperty(), model.createResource(it)) }
        return this
    }

    protected abstract fun containerRdfType(): Resource

    protected abstract fun memberLinkProperty(): Property
}
