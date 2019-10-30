package streams.kafka

import kotlinx.coroutines.*
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.neo4j.kernel.configuration.Config
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.logging.Log
import streams.*
import streams.extensions.offsetAndMetadata
import streams.extensions.toStreamsSinkEntity
import streams.extensions.topicPartition
import streams.service.StreamsSinkEntity
import streams.service.dlq.DLQData
import streams.service.dlq.KafkaDLQService
import streams.utils.Neo4jUtils
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit


class KafkaEventSink(private val config: Config,
                     private val queryExecution: StreamsEventSinkQueryExecution,
                     private val streamsTopicService: StreamsTopicService,
                     private val log: Log,
                     private val db: GraphDatabaseAPI): StreamsEventSink(config, queryExecution, streamsTopicService, log, db) {

    private lateinit var eventConsumer: StreamsEventConsumer
    private lateinit var job: Job

    private val streamsConfigMap = config.raw.filterKeys {
        it.startsWith("kafka.") || (it.startsWith("streams.") && !it.startsWith("streams.sink.topic.cypher."))
    }.toMap()

    private val mappingKeys = mapOf(
            "zookeeper" to "kafka.zookeeper.connect",
            "broker" to "kafka.${ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG}",
            "from" to "kafka.${ConsumerConfig.AUTO_OFFSET_RESET_CONFIG}",
            "autoCommit" to "kafka.${ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG}",
            "groupId" to "kafka.${ConsumerConfig.GROUP_ID_CONFIG}")

    override fun getEventConsumerFactory(): StreamsEventConsumerFactory {
        return object: StreamsEventConsumerFactory() {
            override fun createStreamsEventConsumer(config: Map<String, String>, log: Log): StreamsEventConsumer {
                val kafkaConfig = KafkaSinkConfiguration.from(config)
                val dlqService = if (kafkaConfig.streamsSinkConfiguration.dlqTopic.isNotBlank()) {
                    val asProperties = kafkaConfig.asProperties()
                            .mapKeys { it.key.toString() }
                            .toMutableMap()
                    asProperties.remove(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG)
                    asProperties.remove(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG)
                    KafkaDLQService(asProperties, "__streams.errors")
                } else {
                    null
                }
                return if (kafkaConfig.enableAutoCommit) {
                    KafkaAutoCommitEventConsumer(kafkaConfig, log, dlqService)
                } else {
                    KafkaManualCommitEventConsumer(kafkaConfig, log, dlqService)
                }
            }
        }
    }

    override fun start() { // TODO move to the abstract class
        val streamsConfig = StreamsSinkConfiguration.from(config)
        val topics = streamsTopicService.getTopics()
        val isWriteableInstance = Neo4jUtils.isWriteableInstance(db)
        if (!streamsConfig.enabled) {
            if (topics.isNotEmpty() && isWriteableInstance) {
                log.warn("You configured the following topics: $topics, in order to make the Sink work please set the `${StreamsSinkConfigurationConstants.STREAMS_CONFIG_PREFIX}${StreamsSinkConfigurationConstants.ENABLED}` to `true`")
            }
            return
        }
        log.info("Starting the Kafka Sink")
        this.eventConsumer = getEventConsumerFactory()
                .createStreamsEventConsumer(config.raw, log)
                .withTopics(topics)
        this.eventConsumer.start()
        this.job = createJob()
        if (isWriteableInstance) {
            if (log.isDebugEnabled) {
                log.debug("Subscribed topics with Cypher queries: ${streamsTopicService.getAllCypherTemplates()}")
                log.debug("Subscribed topics with CDC configuration: ${streamsTopicService.getAllCDCTopics()}")
            } else {
                log.info("Subscribed topics: $topics")
            }
        }
        log.info("Kafka Sink started")
    }

    override fun stop() = runBlocking { // TODO move to the abstract class
        log.info("Stopping Kafka Sink daemon Job")
        try {
            job.cancelAndJoin()
            log.info("Kafka Sink daemon Job stopped")
        } catch (e : UninitializedPropertyAccessException) { /* ignoring this one only */ }
    }

    override fun getEventSinkConfigMapper(): StreamsEventSinkConfigMapper { // TODO move to the abstract class
        return object: StreamsEventSinkConfigMapper(streamsConfigMap, mappingKeys) {
            override fun convert(config: Map<String, String>): Map<String, String> {
                val props = streamsConfigMap
                        .toMutableMap()
                props += config.mapKeys { mappingKeys.getOrDefault(it.key, it.key) }
                return props
            }

        }
    }

    private fun createJob(): Job {
        log.info("Creating Sink daemon Job")
        return GlobalScope.launch(Dispatchers.IO) { // TODO improve exception management
            try {
                while (isActive) {
                    val timeMillis = if (Neo4jUtils.isWriteableInstance(db)) {
                        eventConsumer.read { topic, data ->
                            if (log.isDebugEnabled) {
                                log.debug("Reading data from topic $topic")
                            }
                            queryExecution.writeForTopic(topic, data)
                        }
                        0
                    } else {
                        val timeMillis = TimeUnit.MINUTES.toMillis(5)
                        if (log.isDebugEnabled) {
                            log.debug("Not in a writeable instance, new check in $timeMillis millis")
                        }
                        timeMillis
                    }
                    delay(timeMillis)
                }
                eventConsumer.stop()
            } catch (e: Throwable) {
                val message = e.message ?: "Generic error, please check the stack trace: "
                log.error(message, e)
                eventConsumer.stop()
            }
        }
    }

}

data class KafkaTopicConfig(val commit: Boolean, val topicPartitionsMap: Map<TopicPartition, Long>) {
    companion object {
        private fun toTopicPartitionMap(topicConfig: Map<String,
                List<Map<String, Any>>>): Map<TopicPartition, Long> = topicConfig
                .flatMap { topicConfigEntry ->
                    topicConfigEntry.value.map {
                        val partition = it.getValue("partition").toString().toInt()
                        val offset = it.getValue("offset").toString().toLong()
                        TopicPartition(topicConfigEntry.key, partition) to offset
                    }
                }
                .toMap()

        fun fromMap(map: Map<String, Any>): KafkaTopicConfig {
            val commit = map.getOrDefault("commit", true).toString().toBoolean()
            val topicPartitionsMap = toTopicPartitionMap(map
                    .getOrDefault("partitions", emptyMap<String, List<Map<String, Any>>>()) as Map<String, List<Map<String, Any>>>)
            return KafkaTopicConfig(commit = commit, topicPartitionsMap = topicPartitionsMap)
        }
    }
}

open class KafkaAutoCommitEventConsumer(private val config: KafkaSinkConfiguration,
                                        private val log: Log,
                                        private val dlqService: KafkaDLQService?): StreamsEventConsumer(log, dlqService) {

    private var isSeekSet = false

    val consumer = KafkaConsumer<ByteArray, ByteArray>(config.asProperties())

    lateinit var topics: Set<String>

    override fun withTopics(topics: Set<String>): StreamsEventConsumer {
        this.topics = topics
        return this
    }

    override fun start() {
        if (topics.isEmpty()) {
            log.info("No topics specified Kafka Consumer will not started")
            return
        }
        this.consumer.subscribe(topics)
    }

    override fun stop() {
        consumer.close()
        dlqService?.close()
    }

    private fun readSimple(action: (String, List<StreamsSinkEntity>) -> Unit) {
        val records = consumer.poll(0)
        this.topics
                .filter { topic -> records.records(topic).iterator().hasNext() }
                .map { topic -> topic to records.records(topic) }
                .forEach { (topic, topicRecords) -> executeAction(action, topic, topicRecords) }
    }

    fun executeAction(action: (String, List<StreamsSinkEntity>) -> Unit, topic: String,
                      topicRecords: MutableIterable<ConsumerRecord<ByteArray, ByteArray>>) {
        var failedCount = 0
        do {
            try {
                action(topic, convert(topicRecords))
                failedCount = 0
            } catch (e: Exception) {
                topicRecords
                        .map { DLQData.from(it, e, this::class.java) }
                        .forEach { sentToDLQ(it) }
                // 无限重试，记录log
                if (++failedCount % 5 == 0) {
                    log.warn("Write to db failed, try $failedCount times. $e")
                    Thread.sleep(1000)
                }
            }
        } while (failedCount > 0)
    }

    private fun convert(topicRecords: MutableIterable<ConsumerRecord<ByteArray, ByteArray>>): List<StreamsSinkEntity> = topicRecords
            .map {
                try {
                    "ok" to it.toStreamsSinkEntity()
                } catch (e: Exception) {
                    "error" to DLQData.from(it, e, this::class.java)
                }
            }
            .groupBy({ it.first }, { it.second })
            .let {
                it.getOrDefault("error", emptyList<DLQData>())
                        .forEach{ sentToDLQ(it as DLQData) }
                it.getOrDefault("ok", emptyList()) as List<StreamsSinkEntity>
            }

    private fun sentToDLQ(dlqData: DLQData) {
        dlqService?.send(config.streamsSinkConfiguration.dlqTopic, dlqData)
    }

    private fun readFromPartition(config: KafkaTopicConfig, action: (String, List<StreamsSinkEntity>) -> Unit) {
        setSeek(config.topicPartitionsMap)
        val records = consumer.poll(0)
        config.topicPartitionsMap
                .mapValues { records.records(it.key) }
                .filterValues { it.isNotEmpty() }
                .mapKeys { it.key.topic() }
                .forEach { (topic, topicRecords) -> executeAction(action, topic, topicRecords) }
    }

    override fun read(action: (String, List<StreamsSinkEntity>) -> Unit) {
        readSimple(action)
    }

    override fun read(topicConfig: Map<String, Any>, action: (String, List<StreamsSinkEntity>) -> Unit) {
        val kafkaTopicConfig = KafkaTopicConfig.fromMap(topicConfig)
        if (kafkaTopicConfig.topicPartitionsMap.isEmpty()) {
            readSimple(action)
        } else {
            readFromPartition(kafkaTopicConfig, action)
        }
    }

    fun setSeek(topicPartitionsMap: Map<TopicPartition, Long>) {
        if (isSeekSet) {
            return
        }
        isSeekSet = true
        consumer.poll(0) // dummy call see: https://stackoverflow.com/questions/41008610/kafkaconsumer-0-10-java-api-error-message-no-current-assignment-for-partition
        topicPartitionsMap.forEach {
            when (it.value) {
                -1L -> consumer.seekToBeginning(listOf(it.key))
                -2L -> consumer.seekToEnd(listOf(it.key))
                else -> consumer.seek(it.key, it.value)
            }
        }
    }
}

class KafkaManualCommitEventConsumer(private val config: KafkaSinkConfiguration,
                                     private val log: Log,
                                     private val dlqService: KafkaDLQService?): KafkaAutoCommitEventConsumer(config, log, dlqService) {

    private val topicPartitionOffsetMap = ConcurrentHashMap<TopicPartition, OffsetAndMetadata>()

    override fun start() {
        if (topics.isEmpty()) {
            log.info("No topics specified Kafka Consumer will not started")
            return
        }
        this.consumer.subscribe(topics)
    }

    private fun readSimple(action: (String, List<StreamsSinkEntity>) -> Unit): Map<TopicPartition, OffsetAndMetadata> {
        val records = consumer.poll(0)
        return this.topics
                .filter { topic -> records.records(topic).iterator().hasNext() }
                .map { topic -> topic to records.records(topic) }
                .map { (topic, topicRecords) ->
                    executeAction(action, topic, topicRecords)
                    topicRecords.last()
                }
                .map { it.topicPartition() to it.offsetAndMetadata() }
                .toMap()
    }

    private fun readFromPartition(kafkaTopicConfig: KafkaTopicConfig,
                                  action: (String, List<StreamsSinkEntity>) -> Unit): Map<TopicPartition, OffsetAndMetadata> {
        setSeek(kafkaTopicConfig.topicPartitionsMap)
        val records = consumer.poll(0)
        return kafkaTopicConfig.topicPartitionsMap
                .mapValues { records.records(it.key) }
                .filterValues { it.isNotEmpty() }
                .mapKeys { it.key.topic() }
                .map { (topic, topicRecords) ->
                    executeAction(action, topic, topicRecords)
                    topicRecords.last()
                }
                .map { it.topicPartition() to it.offsetAndMetadata() }
                .toMap()
    }

    private fun commitData(commit: Boolean, topicMap: Map<TopicPartition, OffsetAndMetadata>) {
        if (commit) {
            consumer.commitSync(topicMap)
        }
    }

    override fun read(action: (String, List<StreamsSinkEntity>) -> Unit) {
        val topicMap = readSimple(action)
        topicPartitionOffsetMap += topicMap
        commitData(true, topicMap)
    }

    override fun read(topicConfig: Map<String, Any>, action: (String, List<StreamsSinkEntity>) -> Unit) {
        val kafkaTopicConfig = KafkaTopicConfig.fromMap(topicConfig)
        val topicMap = if (kafkaTopicConfig.topicPartitionsMap.isEmpty()) {
            readSimple(action)
        } else {
            readFromPartition(kafkaTopicConfig, action)
        }
        topicPartitionOffsetMap += topicMap
        commitData(kafkaTopicConfig.commit, topicMap)
    }
}