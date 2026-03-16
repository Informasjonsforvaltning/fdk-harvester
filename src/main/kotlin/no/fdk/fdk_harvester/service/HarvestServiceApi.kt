package no.fdk.fdk_harvester.service

import no.fdk.harvest.DataType

/**
 * Interface to allow JDK dynamic proxies for AOP (@Transactional etc.).
 *
 * Using an interface avoids CGLIB/Objenesis edge cases where constructor-injected fields
 * may appear null on proxied instances in some runtime setups.
 */
interface HarvestServiceApi {
    fun executeHarvest(
        dataSourceId: String,
        dataSourceUrl: String,
        dataType: DataType,
        acceptHeader: String,
        runId: String,
        forced: Boolean,
    ): no.fdk.fdk_harvester.model.HarvestReport?

    fun markResourcesAsDeleted(
        sourceUrl: String,
        dataType: DataType,
        dataSourceId: String,
        runId: String,
    ): no.fdk.fdk_harvester.model.HarvestReport

    fun markResourceAsDeletedByFdkId(
        fdkId: String,
        uri: String,
        dataType: DataType,
        runId: String,
        dataSourceId: String
    ): no.fdk.fdk_harvester.model.HarvestReport
}

