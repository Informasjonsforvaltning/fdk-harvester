package no.fdk.fdk_harvester.harvester

import no.fdk.fdk_harvester.error.HarvestErrorCategory
import no.fdk.fdk_harvester.error.HarvestErrorMessageMapper
import no.fdk.fdk_harvester.model.HarvestDataSource
import no.fdk.fdk_harvester.model.HarvestReport
import no.fdk.fdk_harvester.model.HarvestSourceEntity
import no.fdk.fdk_harvester.model.ResourceEntity
import no.fdk.fdk_harvester.repository.HarvestSourceRepository
import no.fdk.fdk_harvester.rdf.*
import org.apache.jena.rdf.model.Model
import org.apache.jena.riot.Lang
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.net.HttpURLConnection
import java.net.URI
import java.nio.charset.Charset
import java.time.Instant
import java.util.*

private const val TEN_MINUTES = 600000
private const val MAX_CONTENT_SIZE = 75 * 1024 * 1024 // 75MB

/**
 * Base harvester: fetches RDF from a [HarvestDataSource], parses it, and delegates type-specific
 * updates to [updateDB]. Subclasses implement harvest for concept, dataset, dataservice, etc.
 */
abstract class BaseHarvester(
    private val harvestSourceRepository: HarvestSourceRepository
) {
    // Getter-only to avoid relying on constructor/field initialization (Spring proxies may bypass it).
    protected val logger: Logger
        get() = LoggerFactory.getLogger(this::class.java)

    /**
     * Fetches content from a URL using the provided data source configuration.
     * 
     * @param source The harvest data source containing URL, accept header, and optional auth header
     * @return The fetched content as a string
     * @throws HarvestException if the HTTP request fails or returns a non-OK status
     */
    protected fun fetchContent(source: HarvestDataSource): String {
        val connection = URI(source.url).toURL().openConnection() as HttpURLConnection
        try {
            connection.setRequestProperty(HttpHeaders.ACCEPT, source.acceptHeaderValue)
            connection.connectTimeout = TEN_MINUTES
            connection.readTimeout = TEN_MINUTES
            
            // Handle authentication header if present
            source.authHeader?.run {
                connection.setRequestProperty(type ?: HttpHeaders.AUTHORIZATION, value ?: "")
            }

            if (connection.responseCode != HttpStatus.OK.value()) {
                throw HarvestException("${source.url} responded with ${connection.responseCode}, harvest will be aborted")
            }

            // Check content length header if available
            val contentLength = connection.contentLengthLong
            if (contentLength > 0 && contentLength > MAX_CONTENT_SIZE) {
                throw HarvestException("${source.url} content size ($contentLength bytes) exceeds maximum allowed size ($MAX_CONTENT_SIZE bytes)")
            }

            // Handle charset encoding if specified
            val charset = if (connection.contentEncoding != null) {
                Charset.forName(connection.contentEncoding)
            } else {
                Charsets.UTF_8
            }

            // Read with size limit tracking
            return connection
                .inputStream
                .readWithSizeLimit(charset)
        } finally {
            connection.disconnect()
        }
    }

    /**
     * Main harvest entry point with common validation and error handling.
     * Subclasses should call this from their public harvest method.
     */
    protected fun validateAndHarvest(
        source: HarvestDataSource,
        harvestDate: Calendar,
        forceUpdate: Boolean,
        runId: String,
        dataType: String,
        requiresAcceptHeader: Boolean = true
    ): HarvestReport? {
        if (source.id == null || source.url == null) {
            logger.error("Harvest source is not valid")
            return null
        }

        if (requiresAcceptHeader && source.acceptHeaderValue == null) {
            logger.error("Harvest source missing acceptHeaderValue")
            return null
        }

        return try {
            logger.debug("Starting harvest of ${source.url}")

            val jenaWriterType = jenaTypeFromAcceptHeader(source.acceptHeaderValue)
            when {
                jenaWriterType == null -> {
                    logger.error(
                        "Not able to harvest from ${source.url}, no accept header supplied",
                        HarvestException(source.url)
                    )
                    HarvestReportBuilder.createErrorReport(
                        dataType = dataType,
                        source = source,
                        errorMessage = HarvestErrorMessageMapper.toUserMessage(
                            category = HarvestErrorCategory.VALIDATION_ERROR,
                            dataSourceUrl = source.url,
                            dataType = null
                        ),
                        harvestDate = harvestDate,
                        runId = runId
                    )
                }
                jenaWriterType == Lang.RDFNULL -> {
                    logger.error(
                        "Not able to harvest from ${source.url}, header ${source.acceptHeaderValue} is not acceptable",
                        HarvestException(source.url)
                    )
                    HarvestReportBuilder.createErrorReport(
                        dataType = dataType,
                        source = source,
                        errorMessage = HarvestErrorMessageMapper.toUserMessage(
                            category = HarvestErrorCategory.VALIDATION_ERROR,
                            dataSourceUrl = source.url,
                            dataType = null
                        ),
                        harvestDate = harvestDate,
                        runId = runId
                    )
                }
                else -> {
                    val harvested = parseRDF(fetchContent(source), jenaWriterType)
                    updateIfChanged(harvested, source, harvestDate, forceUpdate, runId, dataType)
                }
            }
        } catch (ex: Exception) {
            logger.error("Harvest of ${source.url} failed", ex)

            val category = when (ex) {
                is HarvestSourceConflictException ->
                    HarvestErrorCategory.SOURCE_CONFLICT
                is HarvestException ->
                    HarvestErrorCategory.SOURCE_UNAVAILABLE
                else ->
                    HarvestErrorCategory.INTERNAL_ERROR
            }

            HarvestReportBuilder.createErrorReport(
                dataType = dataType,
                source = source,
                errorMessage = HarvestErrorMessageMapper.toUserMessage(
                    category = category,
                    dataSourceUrl = source.url,
                    dataType = null
                ),
                harvestDate = harvestDate,
                runId = runId
            )
        }
    }

    /**
     * Gets or creates a HarvestSourceEntity for the given URI.
     * If it exists, returns it as-is (HarvestSource is immutable).
     * If not, creates a new one.
     */
    protected fun getOrCreateHarvestSource(uri: String, checksum: String, issued: Instant): HarvestSourceEntity {
        val existing = harvestSourceRepository.findByUri(uri)
        return existing ?: harvestSourceRepository.save(HarvestSourceEntity(uri = uri, checksum = checksum, issued = issued, initialized = false))
    }

    /**
     * Checks if the harvested model has changed compared to the stored version using checksums.
     * NOTE: HarvestSource is immutable, so we do not persist source-level checksums for change detection.
     * Change detection is handled at the resource level inside each harvester (via resource checksums).
     */
    protected fun updateIfChanged(
        harvested: Model,
        source: HarvestDataSource,
        harvestDate: Calendar,
        forceUpdate: Boolean,
        runId: String,
        dataType: String
    ): HarvestReport {
        val sourceUrl = source.url!!
        val harvestedChecksum = computeChecksum(harvested)
        logger.info("Updating metadata for $sourceUrl (source checksum computed: $harvestedChecksum)")
        val harvestSource = getOrCreateHarvestSource(sourceUrl, harvestedChecksum, harvestDate.toInstant())
        return updateDB(harvested, source, harvestDate, forceUpdate, runId, dataType, harvestSource)
    }

    /**
     * Validates that a resource URI can be harvested from the given harvest source.
     * Throws HarvestSourceConflictException if the resource already exists, is not removed, and has a different harvest source.
     * 
     * @param resourceUri The URI of the resource to validate
     * @param harvestSource The harvest source attempting to harvest this resource
     * @param dbResource The existing resource from the database (if any)
     * @throws HarvestException if validation fails
     */
    protected fun validateSourceUrl(
        resourceUri: String,
        harvestSource: HarvestSourceEntity,
        dbResource: ResourceEntity?
    ) {
        if (dbResource != null && !dbResource.removed && dbResource.harvestSource.id != harvestSource.id) {
            throw HarvestSourceConflictException(
                "Resource $resourceUri already exists and was harvested from ${dbResource.harvestSource.uri}. " +
                "Cannot harvest from different source ${harvestSource.uri}"
            )
        }
    }

    /**
     * Abstract method for type-specific database update logic.
     * Subclasses implement this to handle their specific resource types.
     */
    protected abstract fun updateDB(
        harvested: Model,
        source: HarvestDataSource,
        harvestDate: Calendar,
        forceUpdate: Boolean,
        runId: String,
        dataType: String,
        harvestSource: HarvestSourceEntity
    ): HarvestReport
}

/**
 * Extension function to read InputStream with a size limit.
 * Throws HarvestException if the content exceeds MAX_CONTENT_SIZE.
 * Reads bytes directly to accurately track size, then converts to string.
 */
private fun InputStream.readWithSizeLimit(charset: Charset): String {
    val byteBuffer = ByteArray(8192)
    val result = ByteArrayOutputStream(MAX_CONTENT_SIZE)
    var totalBytesRead = 0L

    use {
        while (true) {
            val bytesRead = this.read(byteBuffer)
            if (bytesRead == -1) break

            totalBytesRead += bytesRead

            if (totalBytesRead > MAX_CONTENT_SIZE) {
                throw HarvestException("Content size ($totalBytesRead bytes) exceeds maximum allowed size ($MAX_CONTENT_SIZE bytes)")
            }

            result.write(byteBuffer, 0, bytesRead)
        }
    }

    return String(result.toByteArray(), charset)
}

