package no.fdk.harvester.repository

import no.fdk.harvester.model.ResourceEntity
import no.fdk.harvester.model.ResourceType
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Query
import org.springframework.data.repository.query.Param
import org.springframework.stereotype.Repository

/** JPA repository for [ResourceEntity] (by type, fdkId, harvest source). */
@Repository
interface ResourceRepository : JpaRepository<ResourceEntity, String> {
    fun findAllByType(type: ResourceType): List<ResourceEntity>

    fun findAllByTypeAndRemoved(type: ResourceType, removed: Boolean): List<ResourceEntity>

    fun findByFdkId(fdkId: String): ResourceEntity?

    fun findAllByFdkId(fdkId: String): List<ResourceEntity>

    fun findAllByHarvestSourceId(harvestSourceId: Long): List<ResourceEntity>

    @Query(
        "SELECT r.fdkId AS fdkId, r.type AS type, r.harvestSource.uri AS sourceUrl " +
            "FROM ResourceEntity r WHERE r.fdkId IN :fdkIds AND r.removed = false " +
            "AND r.type NOT IN (no.fdk.harvester.model.ResourceType.CATALOG, no.fdk.harvester.model.ResourceType.COLLECTION)",
    )
    fun findSourcesByFdkIds(@Param("fdkIds") fdkIds: Collection<String>): List<ResourceSource>
}

interface ResourceSource {
    val fdkId: String
    val type: ResourceType
    val sourceUrl: String
}
