package no.fdk.harvester.controller

import no.fdk.harvester.Application
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.TestPropertySource
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse

@Tag("unit")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, classes = [Application::class])
@TestPropertySource(
    properties = [
        "spring.jpa.hibernate.ddl-auto=create-drop",
        "spring.flyway.enabled=false",
        "spring.datasource.url=jdbc:h2:mem:fdk_harvester_lookup_test;MODE=PostgreSQL;DB_CLOSE_DELAY=-1;DATABASE_TO_LOWER=TRUE",
        "spring.datasource.driver-class-name=org.h2.Driver",
        "spring.datasource.username=sa",
        "spring.datasource.password=",
        "spring.jpa.properties.hibernate.dialect=org.hibernate.dialect.H2Dialect",
        "spring.kafka.listener.auto-startup=false",
        "spring.kafka.bootstrap-servers=localhost:9092",
        "spring.kafka.consumer.properties.schema.registry.url=http://localhost:8081",
        "app.security.api-key=test-key",
    ],
)
class SourceLookupControllerIntegrationTest {
    @Value("\${local.server.port}")
    private var port: Int = 0

    @Test
    fun `lookup accepts post with api key and without csrf token`() {
        val response = lookup(apiKey = "test-key")

        assertEquals(200, response.statusCode())
        assertTrue(response.body().contains("\"notFound\":[\"unknown\"]"))
    }

    @Test
    fun `lookup rejects request without api key`() {
        assertEquals(401, lookup(apiKey = null).statusCode())
    }

    @Test
    fun `lookup rejects request with wrong api key`() {
        assertEquals(401, lookup(apiKey = "wrong-key").statusCode())
    }

    private fun lookup(apiKey: String?): HttpResponse<String> {
        val builder =
            HttpRequest
                .newBuilder(URI("http://localhost:$port/sources/lookup"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString("""{"fdkIds":["unknown"]}"""))
        apiKey?.let { builder.header("X-API-KEY", it) }
        return HttpClient.newHttpClient().send(builder.build(), HttpResponse.BodyHandlers.ofString())
    }
}
