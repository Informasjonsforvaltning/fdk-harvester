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
    ],
)
class SourceLookupControllerIntegrationTest {
    @Value("\${local.server.port}")
    private var port: Int = 0

    @Test
    fun `lookup accepts post without csrf token`() {
        val request =
            HttpRequest
                .newBuilder(URI("http://localhost:$port/sources/lookup"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString("""{"fdkIds":["unknown"]}"""))
                .build()

        val response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString())

        assertEquals(200, response.statusCode())
        assertTrue(response.body().contains("\"notFound\":[\"unknown\"]"))
    }
}
