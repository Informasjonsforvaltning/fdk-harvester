package no.fdk.fdk_harvester.model

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test

@Tag("unit")
class ResourceTypeTest {

    @Test
    fun `test all resource types exist`() {
        val types = ResourceType.values()
        assertTrue(types.contains(ResourceType.CONCEPT))
        assertTrue(types.contains(ResourceType.COLLECTION))
        assertTrue(types.contains(ResourceType.DATASET))
        assertTrue(types.contains(ResourceType.CATALOG))
        assertTrue(types.contains(ResourceType.EVENT))
        assertTrue(types.contains(ResourceType.SERVICE))
        assertTrue(types.contains(ResourceType.INFORMATIONMODEL))
        assertTrue(types.contains(ResourceType.DATASERVICE))
        assertEquals(8, types.size)
    }

    @Test
    fun `test resource type names`() {
        assertEquals("CONCEPT", ResourceType.CONCEPT.name)
        assertEquals("DATASET", ResourceType.DATASET.name)
        assertEquals("SERVICE", ResourceType.SERVICE.name)
    }
}

