package no.fdk.harvester.error

import no.fdk.harvest.DataType
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test

@Tag("unit")
class HarvestErrorMessageMapperTest {
    @Test
    fun `validation error message is user friendly`() {
        val msg =
            HarvestErrorMessageMapper.toUserMessage(
                category = HarvestErrorCategory.VALIDATION_ERROR,
                dataSourceUrl = "http://example.org/source",
                dataType = DataType.dataset,
            )

        assertTrue(msg.contains("could not start", ignoreCase = true))
    }

    @Test
    fun `source unavailable message contains url`() {
        val url = "http://example.org/source"
        val msg =
            HarvestErrorMessageMapper.toUserMessage(
                category = HarvestErrorCategory.SOURCE_UNAVAILABLE,
                dataSourceUrl = url,
                dataType = DataType.dataset,
            )

        assertTrue(msg.contains(url))
        assertTrue(msg.contains("Unable to harvest data from", ignoreCase = true))
    }

    @Test
    fun `source not found message mentions configuration`() {
        val msg =
            HarvestErrorMessageMapper.toUserMessage(
                category = HarvestErrorCategory.SOURCE_NOT_FOUND,
                dataSourceUrl = "http://example.org/missing",
                dataType = DataType.dataset,
            )

        assertTrue(msg.contains("not found", ignoreCase = true))
    }

    @Test
    fun `source data invalid message mentions format`() {
        val msg =
            HarvestErrorMessageMapper.toUserMessage(
                category = HarvestErrorCategory.SOURCE_DATA_INVALID,
                dataSourceUrl = "http://example.org/source",
                dataType = DataType.dataset,
            )

        assertTrue(msg.contains("valid format", ignoreCase = true))
    }

    @Test
    fun `source conflict message is clear`() {
        val msg =
            HarvestErrorMessageMapper.toUserMessage(
                category = HarvestErrorCategory.SOURCE_CONFLICT,
                dataSourceUrl = "http://example.org/source",
                dataType = DataType.dataset,
            )

        assertTrue(msg.contains("already harvested", ignoreCase = true))
        assertTrue(msg.contains("cannot be harvested", ignoreCase = true))
    }

    @Test
    fun `source conflict message includes original error details`() {
        val originalError =
            "Resource https://dataut.vegvesen.no/ already exists and was harvested from " +
                "https://dataut.vegvesen.no/catalog.ttl. Cannot harvest from different source " +
                "https://dataut.vegvesen.no/api-catalog.ttl"
        val msg =
            HarvestErrorMessageMapper.toUserMessage(
                category = HarvestErrorCategory.SOURCE_CONFLICT,
                dataSourceUrl = "https://dataut.vegvesen.no/api-catalog.ttl",
                dataType = DataType.dataset,
                originalError = originalError,
            )

        assertEquals(originalError, msg)
    }

    @Test
    fun `internal error message is generic`() {
        val msg =
            HarvestErrorMessageMapper.toUserMessage(
                category = HarvestErrorCategory.INTERNAL_ERROR,
                dataSourceUrl = null,
                dataType = null,
            )

        assertTrue(msg.contains("unexpected error", ignoreCase = true))
    }
}
