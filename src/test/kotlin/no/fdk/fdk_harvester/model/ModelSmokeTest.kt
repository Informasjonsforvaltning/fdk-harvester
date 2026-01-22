package no.fdk.fdk_harvester.model

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import java.time.Instant

@Tag("unit")
class ModelSmokeTest {

    @Test
    fun `data classes can be constructed and copied`() {
        val labels = PrefLabel(nb = "nb", nn = "nn", en = "en")
        assertEquals("nb", labels.nb)

        val auth = AuthHeader(name = "n", type = "Authorization", value = "Bearer x")
        assertEquals("Bearer x", auth.value)

        val dup = DuplicateIRI(iriToRemove = "a", iriToRetain = "b", keepRemovedFdkId = false)
        assertFalse(dup.keepRemovedFdkId)

        val hs = HarvestSourceEntity(id = 1L, uri = "http://example.org/source", checksum = "c", issued = Instant.now())
        val re = ResourceEntity(
            uri = "http://example.org/r",
            type = ResourceType.DATASET,
            fdkId = "fdk-1",
            removed = false,
            issued = Instant.now(),
            modified = Instant.now(),
            checksum = "chk",
            harvestSource = hs
        )
        assertEquals("chk", re.checksum)
        assertEquals("fdk-1", re.fdkId)
        assertNotNull(re.issued)
        assertNotNull(re.modified)
    }
}


