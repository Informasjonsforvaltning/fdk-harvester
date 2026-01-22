package no.fdk.fdk_harvester.rdf

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test

@Tag("unit")
class ModellDCATAPNOTest {

    @Test
    fun `namespace constants are defined`() {
        assertEquals("https://data.norge.no/vocabulary/modelldcatno#", ModellDCATAPNO.uri)
        // access properties to count as covered
        assertEquals("${ModellDCATAPNO.uri}model", ModellDCATAPNO.model.uri)
        assertEquals("${ModellDCATAPNO.uri}InformationModel", ModellDCATAPNO.InformationModel.uri)
    }
}




