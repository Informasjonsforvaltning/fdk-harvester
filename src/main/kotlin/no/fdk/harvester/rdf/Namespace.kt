package no.fdk.harvester.rdf

import org.apache.jena.rdf.model.ModelFactory
import org.apache.jena.rdf.model.Property
import org.apache.jena.rdf.model.Resource
import org.apache.jena.rdf.model.ResourceFactory

/** DCAT 2/3 vocabulary (dataset series, etc.). */
class DCAT3 {
    companion object {
        const val URI = "http://www.w3.org/ns/dcat#"

        val DatasetSeries: Resource = ResourceFactory.createResource("${URI}DatasetSeries")
        val inSeries: Property = ResourceFactory.createProperty("${URI}inSeries")
    }
}

/** CPSV vocabulary (Public Service). */
class CPSV {
    companion object {
        private val m = ModelFactory.createDefaultModel()
        const val URI = "http://purl.org/vocab/cpsv#"
        val PublicService: Property = m.createProperty("${URI}PublicService")
    }
}

/** Norwegian CPSV extension (Service). */
class CPSVNO {
    companion object {
        private val m = ModelFactory.createDefaultModel()
        const val URI = "https://data.norge.no/vocabulary/cpsvno#"
        val Service: Property = m.createProperty("${URI}Service")
    }
}

/** CV (Core Vocabulary) event types. */
class CV {
    companion object {
        private val m = ModelFactory.createDefaultModel()
        const val URI = "http://data.europa.eu/m8g/"
        val Event: Property = m.createProperty("${URI}Event")
        val BusinessEvent: Property = m.createProperty("${URI}BusinessEvent")
        val LifeEvent: Property = m.createProperty("${URI}LifeEvent")
        val Participation: Property = m.createProperty("${URI}Participation")
        val playsRole: Property = m.createProperty("${URI}playsRole")
    }
}

/** Norwegian DCAT extension (containsService, containsEvent). */
class DCATNO {
    companion object {
        private val m = ModelFactory.createDefaultModel()
        const val URI = "https://data.norge.no/vocabulary/dcatno#"
        val containsService: Property = m.createProperty("${URI}containsService")
        val containsEvent: Property = m.createProperty("${URI}containsEvent")
    }
}
