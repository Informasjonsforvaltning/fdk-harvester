package no.fdk.fdk_harvester.rdf

import org.apache.jena.rdf.model.ModelFactory
import org.apache.jena.rdf.model.Property

/** Norwegian modelldcatno vocabulary (InformationModel, CodeList, etc.). */
class ModellDCATAPNO {
    companion object {
        private val m = ModelFactory.createDefaultModel()
        val uri = "https://data.norge.no/vocabulary/modelldcatno#"
        val InformationModel: Property = m.createProperty("${uri}InformationModel")
        val CodeList: Property = m.createProperty("${uri}CodeList")
        val CodeElement: Property = m.createProperty("${uri}CodeElement")
        val model: Property = m.createProperty("${uri}model")
    }
}

