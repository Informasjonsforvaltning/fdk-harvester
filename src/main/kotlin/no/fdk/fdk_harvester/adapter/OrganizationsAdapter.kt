package no.fdk.fdk_harvester.adapter

import no.fdk.fdk_harvester.model.Organization

/** Fetches organization/publisher metadata by id (used during harvest). */
interface OrganizationsAdapter {
    fun getOrganization(id: String): Organization?
}

