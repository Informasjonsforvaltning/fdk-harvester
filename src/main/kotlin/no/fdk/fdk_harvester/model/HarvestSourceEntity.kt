package no.fdk.fdk_harvester.model

import jakarta.persistence.*
import java.time.Instant

/** JPA entity for a harvest source (URI, checksum, issued). */
@Entity
@Table(name = "harvest_source")
data class HarvestSourceEntity(
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    val id: Long? = null,

    @Column(name = "uri", nullable = false, unique = true, length = 2048)
    val uri: String,

    @Column(name = "checksum", nullable = false, length = 64)
    val checksum: String,

    @Column(name = "issued", nullable = false, columnDefinition = "TIMESTAMP")
    val issued: Instant
)

