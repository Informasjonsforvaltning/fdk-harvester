package no.fdk.harvester.model

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.EnumType
import jakarta.persistence.Enumerated
import jakarta.persistence.FetchType
import jakarta.persistence.Id
import jakarta.persistence.JoinColumn
import jakarta.persistence.ManyToOne
import jakarta.persistence.Table
import java.time.Instant

/** JPA entity for a harvested resource (URI, type, fdkId, removed flag, timestamps, harvest source). */
@Entity
@Table(name = "resources")
data class ResourceEntity(
    @Id
    @Column(name = "uri", length = 2048)
    val uri: String,
    @Enumerated(EnumType.STRING)
    @Column(name = "type", nullable = false, length = 50)
    val type: ResourceType,
    @Column(name = "fdk_id", nullable = false, length = 255)
    val fdkId: String,
    @Column(name = "removed", nullable = false)
    val removed: Boolean = false,
    @Column(name = "issued", nullable = false, columnDefinition = "TIMESTAMP")
    val issued: Instant,
    @Column(name = "modified", nullable = false, columnDefinition = "TIMESTAMP")
    val modified: Instant,
    @Column(name = "checksum", nullable = false, length = 64)
    val checksum: String,
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "harvest_source_id", nullable = false)
    val harvestSource: HarvestSourceEntity,
)
