package no.fdk.harvester.harvester

import io.mockk.mockk
import no.fdk.harvester.adapter.DefaultOrganizationsAdapter
import no.fdk.harvester.config.ApplicationProperties
import no.fdk.harvester.kafka.ResourceEventProducer
import no.fdk.harvester.model.Organization
import no.fdk.harvester.model.ResourceType
import no.fdk.harvester.repository.HarvestSourceRepository
import no.fdk.harvester.repository.ResourceRepository
import org.apache.jena.rdf.model.Model
import org.apache.jena.rdf.model.ModelFactory
import org.apache.jena.rdf.model.Property
import org.apache.jena.rdf.model.RDFNode
import org.apache.jena.rdf.model.Resource
import org.apache.jena.sparql.vocabulary.FOAF
import org.apache.jena.vocabulary.DCAT
import org.apache.jena.vocabulary.DCTerms
import org.apache.jena.vocabulary.RDF
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import no.fdk.harvest.DataType as HarvestDataType

@Tag("unit")
class ResourceHarvesterTest {
    private lateinit var harvester: TestHarvester

    @BeforeEach
    fun setUp() {
        harvester =
            TestHarvester(
                harvestSourceRepository = mockk(relaxed = true),
                resourceRepository = mockk(relaxed = true),
                applicationProperties = ApplicationProperties(),
                orgAdapter = mockk(relaxed = true),
                resourceEventProducer = mockk(relaxed = true),
            )
    }

    @Test
    fun `keeps literal properties of the container`() {
        val model = ModelFactory.createDefaultModel()
        val container =
            model
                .createResource("http://example.org/catalog")
                .addProperty(RDF.type, DCAT.Catalog)
                .addProperty(DCTerms.title, model.createLiteral("My catalog", "en"))

        val result = harvester.testExtractContainerModel(container, DCAT.service)

        assertTrue(
            result.contains(
                result.getResource("http://example.org/catalog"),
                DCTerms.title,
                result.createLiteral("My catalog", "en"),
            ),
        )
        assertTrue(
            result.contains(
                result.getResource("http://example.org/catalog"),
                RDF.type,
                DCAT.Catalog,
            ),
        )
    }

    @Test
    fun `ignores member-link statements`() {
        val model = ModelFactory.createDefaultModel()
        val member = model.createResource("http://example.org/dataservice")
        val container =
            model
                .createResource("http://example.org/catalog")
                .addProperty(RDF.type, DCAT.Catalog)
                .addProperty(DCAT.service, member)

        val result = harvester.testExtractContainerModel(container, DCAT.service)

        assertFalse(
            result.contains(
                result.getResource("http://example.org/catalog"),
                DCAT.service,
                result.getResource("http://example.org/dataservice"),
            ),
        )
    }

    @Test
    fun `keeps non-member resource property and recursively inlines its graph`() {
        val model = ModelFactory.createDefaultModel()
        val publisher =
            model
                .createResource("http://example.org/publisher")
                .addProperty(RDF.type, FOAF.Agent)
                .addProperty(FOAF.name, model.createLiteral("Publisher name"))
        val container =
            model
                .createResource("http://example.org/catalog")
                .addProperty(RDF.type, DCAT.Catalog)
                .addProperty(DCTerms.publisher, publisher)

        val result = harvester.testExtractContainerModel(container, DCAT.service)

        assertTrue(
            result.contains(
                result.getResource("http://example.org/catalog"),
                DCTerms.publisher,
                result.getResource("http://example.org/publisher"),
            ),
        )
        assertTrue(
            result.contains(
                result.getResource("http://example.org/publisher"),
                FOAF.name,
                result.createLiteral("Publisher name"),
            ),
        )
        assertTrue(
            result.contains(
                result.getResource("http://example.org/publisher"),
                RDF.type,
                FOAF.Agent,
            ),
        )
    }

    @Test
    fun `does not inline separately harvested member types reachable via other properties`() {
        val model = ModelFactory.createDefaultModel()
        val relatedService =
            model
                .createResource("http://example.org/dataservice")
                .addProperty(RDF.type, DCAT.DataService)
                .addProperty(DCTerms.title, model.createLiteral("A service"))
        val container =
            model
                .createResource("http://example.org/catalog")
                .addProperty(RDF.type, DCAT.Catalog)
                .addProperty(DCTerms.relation, relatedService)

        val result = harvester.testExtractContainerModel(container, DCAT.service)

        // The reference is kept, but the separately-harvested service's own graph is not inlined.
        assertTrue(
            result.contains(
                result.getResource("http://example.org/catalog"),
                DCTerms.relation,
                result.getResource("http://example.org/dataservice"),
            ),
        )
        assertFalse(
            result.contains(
                result.getResource("http://example.org/dataservice"),
                DCTerms.title,
                result.createLiteral("A service"),
            ),
        )
    }

    @Test
    fun `returns an empty model for a container with no properties`() {
        val model = ModelFactory.createDefaultModel()
        val container = model.createResource("http://example.org/catalog")

        val result = harvester.testExtractContainerModel(container, DCAT.service)

        assertTrue(result.isEmpty)
    }

    private class TestHarvester(
        harvestSourceRepository: HarvestSourceRepository,
        resourceRepository: ResourceRepository,
        applicationProperties: ApplicationProperties,
        orgAdapter: DefaultOrganizationsAdapter,
        resourceEventProducer: ResourceEventProducer,
    ) : ResourceHarvester(
            harvestSourceRepository,
            resourceRepository,
            applicationProperties,
            orgAdapter,
            resourceEventProducer,
        ) {
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

        override fun memberLinkProperty(): Property = DCAT.service

        override fun listMembers(
            harvested: Model,
            sourceURL: String,
        ): List<MemberRDFModel> = emptyList()

        override fun extractContainers(
            harvested: Model,
            members: List<MemberRDFModel>,
            sourceURL: String,
            organization: Organization?,
        ): List<ContainerRDFModel> = emptyList()

        override fun isSeparatelyHarvestedMemberType(types: List<RDFNode>): Boolean = types.contains(DCAT.DataService)

        fun testExtractContainerModel(
            resource: Resource,
            memberLinkProperty: Property,
        ): Model = resource.extractContainerModel(memberLinkProperty)
    }
}
