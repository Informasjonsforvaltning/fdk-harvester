package no.fdk.fdk_harvester.error

/**
 * High-level, user-facing harvest error categories.
 *
 * These categories are intentionally coarse so they can be mapped to
 * friendly GUI messages while keeping technical details in the logs.
 */
enum class HarvestErrorCategory {
    /**
     * Problems with the incoming harvest request/event itself, such as
     * missing required fields.
     */
    VALIDATION_ERROR,

    /**
     * The configured harvest source (URL / endpoint) cannot be reached
     * due to network issues, timeouts, or server errors.
     */
    SOURCE_UNAVAILABLE,

    /**
     * The configured harvest source does not exist or is not found in
     * configuration or remote endpoint (e.g. HTTP 404).
     */
    SOURCE_NOT_FOUND,

    /**
     * The data returned from the source is syntactically or semantically
     * invalid and cannot be processed.
     */
    SOURCE_DATA_INVALID,

    /**
     * The resource is already harvested from another source and cannot
     * be harvested from the current source.
     */
    SOURCE_CONFLICT,

    /**
     * Any unexpected error that occurs internally in the harvester, such
     * as database failures or unhandled exceptions.
     */
    INTERNAL_ERROR,
}

