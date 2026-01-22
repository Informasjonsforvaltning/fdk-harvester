package no.fdk.fdk_harvester

import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.ConfigurationPropertiesScan

/**
 * FDK Harvester application entry point.
 *
 * Consumes harvest commands from Kafka (harvest-events topic), runs RDF harvests for the given
 * data source and data type (concept, dataset, dataservice, informationmodel, service, event),
 * and publishes resource events to type-specific Kafka topics.
 *
 * No REST API is exposed; interaction is via Kafka and actuator endpoints (health, info, prometheus).
 */
@SpringBootApplication
@ConfigurationPropertiesScan
open class Application

/**
 * Application main entry point.
 */
fun main(args: Array<String>) {
    SpringApplication.run(Application::class.java, *args)
}

