package no.fdk.fdk_harvester.adapter

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock.aResponse
import com.github.tomakehurst.wiremock.client.WireMock.get
import com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import io.mockk.every
import io.mockk.mockk
import no.fdk.fdk_harvester.config.ApplicationProperties
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test

@Tag("unit")
class DefaultOrganizationsAdapterTest {

    @Test
    fun `getOrganization returns parsed org and sets uri`() {
        val server = WireMockServer(wireMockConfig().dynamicPort())
        server.start()
        try {
            server.stubFor(
                get(urlEqualTo("/organizations/123"))
                    .willReturn(
                        aResponse()
                            .withStatus(200)
                            .withHeader("Content-Type", "application/json")
                            .withBody("""{ "organizationId": "123", "name": "Org" }""")
                    )
            )

            val props = mockk<ApplicationProperties>()
            every { props.organizationsUri } returns "http://localhost:${server.port()}/organizations"

            val adapter = DefaultOrganizationsAdapter(props)
            val org = adapter.getOrganization("123")

            assertNotNull(org)
            assertEquals("123", org!!.organizationId)
            assertEquals("Org", org.name)
            assertEquals("http://localhost:${server.port()}/organizations/123", org.uri)
        } finally {
            server.stop()
        }
    }

    @Test
    fun `getOrganization returns null on non-2xx`() {
        val server = WireMockServer(wireMockConfig().dynamicPort())
        server.start()
        try {
            server.stubFor(get(urlEqualTo("/organizations/123")).willReturn(aResponse().withStatus(500)))

            val props = mockk<ApplicationProperties>()
            every { props.organizationsUri } returns "http://localhost:${server.port()}/organizations"

            val adapter = DefaultOrganizationsAdapter(props)
            val org = adapter.getOrganization("123")
            assertNull(org)
        } finally {
            server.stop()
        }
    }

    @Test
    fun `getOrganization returns null on connection error`() {
        val props = mockk<ApplicationProperties>()
        // valid URI but unreachable -> triggers exception inside try/catch
        every { props.organizationsUri } returns "http://localhost:1/organizations"

        val adapter = DefaultOrganizationsAdapter(props)
        val org = adapter.getOrganization("123")
        assertNull(org)
    }
}


