package no.fdk.fdk_harvester.kafka

import io.confluent.kafka.serializers.KafkaAvroDeserializer
import io.confluent.kafka.serializers.KafkaAvroSerializer
import no.fdk.harvest.HarvestEvent
import org.apache.avro.specific.SpecificRecord
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.core.ProducerFactory
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.listener.ContainerProperties

/**
 * Kafka configuration: consumer for harvest-events (Avro [HarvestEvent]),
 * producer factories for harvest-events and for resource events (dataset, concept, etc.).
 */
@Configuration
@EnableKafka
open class KafkaConfig(
    @param:Value("\${spring.kafka.bootstrap-servers}") private val bootstrapServers: String,
    @param:Value("\${spring.kafka.consumer.properties.schema.registry.url}") private val schemaRegistryUrl: String,
) {
    @Bean
    open fun consumerFactory(): ConsumerFactory<String, HarvestEvent> {
        val props: MutableMap<String, Any> = HashMap()
        props[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServers
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
        factory.setConcurrency(4)
        return factory
    }

    @Bean
    open fun resourceEventProducerFactory(): ProducerFactory<String, SpecificRecord> {
        val props: MutableMap<String, Any> = HashMap()
        props[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServers
        props[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
        props[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = KafkaAvroSerializer::class.java
        props[ProducerConfig.COMPRESSION_TYPE_CONFIG] = "snappy"
        props["schema.registry.url"] = schemaRegistryUrl
        props["value.subject.name.strategy"] = "io.confluent.kafka.serializers.subject.RecordNameStrategy"
        return DefaultKafkaProducerFactory(props)
    }

    @Bean
    open fun resourceEventKafkaTemplate(): KafkaTemplate<String, SpecificRecord> {
        return KafkaTemplate(resourceEventProducerFactory())
    }

    @Bean
    open fun harvestEventProducerFactory(): ProducerFactory<String, HarvestEvent> {
        val props: MutableMap<String, Any> = HashMap()
        props[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServers
        props[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
        props[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = KafkaAvroSerializer::class.java
        props[ProducerConfig.COMPRESSION_TYPE_CONFIG] = "snappy"
        props["schema.registry.url"] = schemaRegistryUrl
        props["value.subject.name.strategy"] = "io.confluent.kafka.serializers.subject.RecordNameStrategy"
        return DefaultKafkaProducerFactory(props)
    }

    @Bean
    open fun harvestEventKafkaTemplate(): KafkaTemplate<String, HarvestEvent> {
        return KafkaTemplate(harvestEventProducerFactory())
    }
}

