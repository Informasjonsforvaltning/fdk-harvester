package no.fdk.fdk_harvester.rdf

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test

@Tag("unit")
class NamespaceSmokeTest {

    @Test
    fun `namespace objects expose expected uris`() {
        assertEquals("http://data.europa.eu/m8g/", CV.uri)
        assertEquals("${CV.uri}playsRole", CV.playsRole.uri)
        assertEquals("https://data.norge.no/vocabulary/cpsvno#", CPSVNO.uri)
        assertEquals("${CPSVNO.uri}Service", CPSVNO.Service.uri)
    }
}




