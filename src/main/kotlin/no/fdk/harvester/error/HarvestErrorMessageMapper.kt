package no.fdk.harvester.error

import no.fdk.harvest.DataType

/**
 * Maps harvest error categories and simple context into user-friendly English
 * messages suitable for display in the GUI.
 *
 * Technical details and stack traces must be logged separately and should not
 * be included in these messages. When an [originalError] is available it is
 * appended so operators can see actionable detail without opening logs.
 */
object HarvestErrorMessageMapper {
    fun toUserMessage(
        category: HarvestErrorCategory,
        dataSourceUrl: String? = null,
        dataType: DataType? = null,
        originalError: String? = null,
    ): String = when (category) {
        HarvestErrorCategory.VALIDATION_ERROR -> {
            withOriginalError(
                "This harvest could not start because required information is missing or invalid.",
                originalError,
            )
        }

        HarvestErrorCategory.SOURCE_UNAVAILABLE -> {
            "Unable to harvest data from the source${urlFragment(
                dataSourceUrl,
            )}. Original error message: '$originalError'. Please remedy and/or check that the service is available and try again."
        }

        HarvestErrorCategory.SOURCE_NOT_FOUND -> {
            withOriginalError(
                "The configured data source${urlFragment(dataSourceUrl)} was not found. It may have been removed or misconfigured.",
                originalError,
            )
        }

        HarvestErrorCategory.SOURCE_DATA_INVALID -> {
            withOriginalError(
                "The data from${urlFragment(
                    dataSourceUrl,
                )} could not be imported because it is not in a valid format. Please verify the published ${dataTypeFragment(
                    dataType,
                )}data.",
                originalError,
            )
        }

        HarvestErrorCategory.SOURCE_CONFLICT -> {
            originalError?.takeIf { it.isNotBlank() }
                ?: (
                    "A resource present in the source is already harvested from another data source " +
                        "and cannot be harvested from the current source."
                    )
        }

        HarvestErrorCategory.INTERNAL_ERROR -> {
            withOriginalError(
                "An unexpected error occurred during harvesting. Please try again later or contact support if the problem persists.",
                originalError,
            )
        }
    }

    private fun withOriginalError(base: String, originalError: String?): String {
        val detail = originalError?.takeIf { it.isNotBlank() } ?: return base
        return "$base Original error message: '$detail'."
    }

    private fun urlFragment(url: String?): String = if (url.isNullOrBlank()) "" else " at $url"

    private fun dataTypeFragment(dataType: DataType?): String = if (dataType == null) "" else "${dataType.name.lowercase()} "
}
