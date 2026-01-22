package no.fdk.fdk_harvester.config

import org.springframework.boot.context.properties.ConfigurationProperties

/**
 * Application-level configuration properties (prefix: `application`).
 *
 * @property organizationsUri Base URL for the organizations API used when resolving publisher/organization metadata during harvest.
 * @property datasetUri Base URL for FDK dataset URIs (e.g. https://datasets.fellesdatakatalog.digdir.no/datasets). Catalog FDK base is derived as datasetUri parent + /catalogs.
 * @property dataserviceUri Base URL for FDK dataservice URIs. Catalog FDK base derived as parent + /catalogs.
 * @property informationmodelUri Base URL for FDK information model URIs. Catalog FDK base derived as parent + /catalogs.
 * @property eventUri Base URL for FDK event URIs. Catalog FDK base derived as parent + /catalogs.
 * @property serviceUri Base URL for FDK service URIs. Catalog FDK base derived as parent + /catalogs.
 * @property conceptUri Base URL for FDK concept URIs (e.g. https://concepts.fellesdatakatalog.digdir.no/concepts). Collection FDK base derived as parent + /collections.
 */
@ConfigurationProperties("application")
data class ApplicationProperties(
    val organizationsUri: String? = null,
    val datasetUri: String = "https://datasets.fellesdatakatalog.digdir.no/datasets",
    val dataserviceUri: String = "https://dataservices.fellesdatakatalog.digdir.no/dataservices",
    val informationmodelUri: String = "https://informationmodels.fellesdatakatalog.digdir.no/informationmodels",
    val eventUri: String = "https://events.fellesdatakatalog.digdir.no/events",
    val serviceUri: String = "https://services.fellesdatakatalog.digdir.no/services",
    val conceptUri: String = "https://concepts.fellesdatakatalog.digdir.no/concepts"
)

