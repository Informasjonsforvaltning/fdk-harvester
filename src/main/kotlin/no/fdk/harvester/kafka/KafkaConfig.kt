package no.fdk.harvester.kafka

import io.confluent.kafka.serializers.KafkaAvroDeserializer
import io.confluent.kafka.serializers.KafkaAvroSerializer
import no.fdk.harvest.HarvestEvent
import org.apache.avro.specific.SpecificRecord
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.core.ProducerFactory
import org.springframework.kafka.listener.ContainerProperties
import java.time.Duration

/**
 * Kafka configuration: consumer for harvest-events (Avro [HarvestEvent]),
 * producer factories for harvest-events and for resource events (dataset, concept, etc.).
 */
@Configuration
@EnableKafka
open class KafkaConfig(
    @param:Value("\${spring.kafka.bootstrap-servers}") private val bootstrapServers: String,
    @param:Value("\${spring.kafka.properties.security.protocol}") private val securityProtocol: String,
    @param:Value("\${spring.kafka.properties.sasl.mechanism}") private val saslMechanism: String,
    @param:Value("\${spring.kafka.properties.sasl.jaas.config}") private val saslJaasConfig: String,
    @param:Value("\${spring.kafka.consumer.properties.schema.registry.url}") private val schemaRegistryUrl: String,
) {
    @Bean
    open fun consumerFactory(): ConsumerFactory<String, HarvestEvent> {
        val props: MutableMap<String, Any> = HashMap()
        props[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServers
        props[CommonClientConfigs.SECURITY_PROTOCOL_CONFIG] = securityProtocol
        props[SaslConfigs.SASL_MECHANISM] = saslMechanism
        props[SaslConfigs.SASL_JAAS_CONFIG] = saslJaasConfig
        props[ConsumerConfig.GROUP_ID_CONFIG] = "fdk-harvester"
        props[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        props[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = KafkaAvroDeserializer::class.java
        props["schema.registry.url"] = schemaRegistryUrl
        props["specific.avro.reader"] = true
        props[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"
        return DefaultKafkaConsumerFactory(props)
    }

    @Bean
    open fun kafkaListenerContainerFactory(): ConcurrentKafkaListenerContainerFactory<String, HarvestEvent> {
        val factory = ConcurrentKafkaListenerContainerFactory<String, HarvestEvent>()
        factory.setConsumerFactory(consumerFactory())
        factory.containerProperties.ackMode = ContainerProperties.AckMode.MANUAL
        // Keep retrying instead of stopping the listeners for good on an auth failure.
        factory.containerProperties.setAuthExceptionRetryInterval(Duration.ofSeconds(30))
        factory.setConcurrency(4)
        return factory
    }

    @Bean
    open fun resourceEventProducerFactory(): ProducerFactory<String, SpecificRecord> {
        val props: MutableMap<String, Any> = HashMap()
        props[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServers
        props[CommonClientConfigs.SECURITY_PROTOCOL_CONFIG] = securityProtocol
        props[SaslConfigs.SASL_MECHANISM] = saslMechanism
        props[SaslConfigs.SASL_JAAS_CONFIG] = saslJaasConfig
        props[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
        props[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = KafkaAvroSerializer::class.java
        props[ProducerConfig.COMPRESSION_TYPE_CONFIG] = "snappy"
        props["schema.registry.url"] = schemaRegistryUrl
        props["value.subject.name.strategy"] = "io.confluent.kafka.serializers.subject.RecordNameStrategy"
        return DefaultKafkaProducerFactory(props)
    }

    @Bean
    open fun resourceEventKafkaTemplate(): KafkaTemplate<String, SpecificRecord> = KafkaTemplate(resourceEventProducerFactory())

    @Bean
    open fun harvestEventProducerFactory(): ProducerFactory<String, HarvestEvent> {
        val props: MutableMap<String, Any> = HashMap()
        props[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServers
        props[CommonClientConfigs.SECURITY_PROTOCOL_CONFIG] = securityProtocol
        props[SaslConfigs.SASL_MECHANISM] = saslMechanism
        props[SaslConfigs.SASL_JAAS_CONFIG] = saslJaasConfig
        props[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
        props[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = KafkaAvroSerializer::class.java
        props[ProducerConfig.COMPRESSION_TYPE_CONFIG] = "snappy"
        props["schema.registry.url"] = schemaRegistryUrl
        props["value.subject.name.strategy"] = "io.confluent.kafka.serializers.subject.RecordNameStrategy"
        return DefaultKafkaProducerFactory(props)
    }

    @Bean
    open fun harvestEventKafkaTemplate(): KafkaTemplate<String, HarvestEvent> = KafkaTemplate(harvestEventProducerFactory())
}
