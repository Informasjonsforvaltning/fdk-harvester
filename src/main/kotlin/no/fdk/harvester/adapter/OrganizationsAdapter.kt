package no.fdk.harvester.adapter

import no.fdk.harvester.model.Organization

/** Fetches organization/publisher metadata by id (used during harvest). */
interface OrganizationsAdapter {
    fun getOrganization(id: String): Organization?
}
