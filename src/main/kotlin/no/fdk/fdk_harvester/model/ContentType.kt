package no.fdk.fdk_harvester.model

enum class ContentType(vararg val headers: String) {
    TURTLE("text/turtle", "application/turtle"),
    N3("text/rdf+n3", "application/n3", "text/n3"),
    TRIG("application/trig", "text/trig"),
    RDFXML("application/rdf+xml"),
    RDFJSON("application/rdf+json"),
    JSONLD("application/ld+json"),
    NTRIPLES("application/n-triples", "text/plain"),
    NQUADS("application/n-quads", "text/n-quads"),
    TRIX("application/trix", "application/trix+xml");

    fun isRDF(): Boolean {
        return listOf(TURTLE, N3, TRIG, RDFXML, RDFJSON, JSONLD, NTRIPLES, NQUADS, TRIX).contains(this)
    }

    companion object {
        fun fromValue(contentType: String?): ContentType? {
            return values().find { it.headers.contains(contentType?.lowercase()) }
        }
    }
}

