package no.fdk.fdk_harvester.rdf

import org.apache.jena.rdf.model.Property
import org.apache.jena.rdf.model.Resource
import org.apache.jena.rdf.model.ResourceFactory

import org.apache.jena.rdf.model.ModelFactory

/** DCAT 2/3 vocabulary (dataset series, etc.). */
class DCAT3 {
    companion object {
        const val uri = "http://www.w3.org/ns/dcat#"

        val DatasetSeries: Resource = ResourceFactory.createResource("${uri}DatasetSeries")
        val inSeries: Property = ResourceFactory.createProperty("${uri}inSeries")
    }
}

/** CPSV vocabulary (Public Service). */
class CPSV {
    companion object {
        private val m = ModelFactory.createDefaultModel()
        const val uri = "http://purl.org/vocab/cpsv#"
        val PublicService: Property = m.createProperty("${uri}PublicService")
    }
}

/** Norwegian CPSV extension (Service). */
class CPSVNO {
    companion object {
        private val m = ModelFactory.createDefaultModel()
        const val uri = "https://data.norge.no/vocabulary/cpsvno#"
        val Service: Property = m.createProperty("${uri}Service")
    }
}

/** CV (Core Vocabulary) event types. */
class CV {
    companion object {
        private val m = ModelFactory.createDefaultModel()
        const val uri = "http://data.europa.eu/m8g/"
        val Event: Property = m.createProperty("${uri}Event")
        val BusinessEvent: Property = m.createProperty("${uri}BusinessEvent")
        val LifeEvent: Property = m.createProperty("${uri}LifeEvent")
        val Participation: Property = m.createProperty("${uri}Participation")
        val playsRole: Property = m.createProperty("${uri}playsRole")
    }
}

/** Norwegian DCAT extension (containsService, containsEvent). */
class DCATNO {
    companion object {
        private val m = ModelFactory.createDefaultModel()
        const val uri = "https://data.norge.no/vocabulary/dcatno#"
        val containsService: Property = m.createProperty("${uri}containsService")
        val containsEvent: Property = m.createProperty("${uri}containsEvent")
    }
}
