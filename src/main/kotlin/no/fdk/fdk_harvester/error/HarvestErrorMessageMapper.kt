package no.fdk.fdk_harvester.error

import no.fdk.harvest.DataType

/**
 * Maps harvest error categories and simple context into user-friendly English
 * messages suitable for display in the GUI.
 *
 * Technical details and stack traces must be logged separately and should not
 * be included in these messages.
 */
object HarvestErrorMessageMapper {

    fun toUserMessage(
        category: HarvestErrorCategory,
        dataSourceUrl: String? = null,
        dataType: DataType? = null,
        originalError: String? = null,
    ): String =
        when (category) {
            HarvestErrorCategory.VALIDATION_ERROR ->
                "This harvest could not start because required information is missing or invalid."

            HarvestErrorCategory.SOURCE_UNAVAILABLE ->
                "Unable to harvest data from the source${urlFragment(dataSourceUrl)}. Original error message: '$originalError'. Please remedy and/or check that the service is available and try again."

            HarvestErrorCategory.SOURCE_NOT_FOUND ->
                "The configured data source${urlFragment(dataSourceUrl)} was not found. It may have been removed or misconfigured."

            HarvestErrorCategory.SOURCE_DATA_INVALID ->
                "The data from${urlFragment(dataSourceUrl)} could not be imported because it is not in a valid format. Please verify the published ${dataTypeFragment(dataType)}data."

            HarvestErrorCategory.SOURCE_CONFLICT ->
                "This resource is already harvested from another data source and cannot be harvested from the current source."

            HarvestErrorCategory.INTERNAL_ERROR ->
                "An unexpected error occurred during harvesting. Please try again later or contact support if the problem persists."
        }

    private fun urlFragment(url: String?): String =
        if (url.isNullOrBlank()) "" else " at $url"

    private fun dataTypeFragment(dataType: DataType?): String =
        if (dataType == null) "" else "${dataType.name.lowercase()} "
}

