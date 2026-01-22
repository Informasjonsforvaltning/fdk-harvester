package no.fdk.fdk_harvester.repository

import no.fdk.fdk_harvester.model.ResourceEntity
import no.fdk.fdk_harvester.model.ResourceType
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.stereotype.Repository

/** JPA repository for [ResourceEntity] (by type, fdkId, harvest source). */
@Repository
interface ResourceRepository : JpaRepository<ResourceEntity, String> {
    fun findAllByType(type: ResourceType): List<ResourceEntity>
    fun findAllByTypeAndRemoved(type: ResourceType, removed: Boolean): List<ResourceEntity>
    fun findByFdkId(fdkId: String): ResourceEntity?
    fun findAllByFdkId(fdkId: String): List<ResourceEntity>
    fun findAllByHarvestSourceId(harvestSourceId: Long): List<ResourceEntity>
}

