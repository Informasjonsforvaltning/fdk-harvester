# fdk-harvester

Kafka-based harvester for the Felles datakatalog (FDK). Consumes harvest commands from the `harvest-events` topic, fetches RDF from the configured data source, and publishes resource events to type-specific topics (dataset, concept, dataservice, informationmodel, service, event).

## API and documentation

**This service does not expose a REST API.** There is no OpenAPI/Swagger spec to maintain.

- **Integration** is via **Kafka**: consume `HarvestEvent` (Avro) from `harvest-events`; the service produces harvest-result events and resource-specific events to their respective topics.
- **Operational endpoints** are Spring Boot Actuator (HTTP):
  - `GET /actuator/health` – liveness/readiness
  - `GET /actuator/info` – build info
  - `GET /actuator/prometheus` – metrics

Event schemas are defined in `kafka/schemas/` (Avro). Code is documented with KDoc (Kotlin doc comments) on public types and main functions.