package no.fdk.harvester.repository

import no.fdk.harvester.model.HarvestSourceEntity
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.stereotype.Repository

/** JPA repository for [HarvestSourceEntity] (lookup by URI). */
@Repository
interface HarvestSourceRepository : JpaRepository<HarvestSourceEntity, Long> {
    fun findByUri(uri: String): HarvestSourceEntity?
}
