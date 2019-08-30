package streams.wal

import org.neo4j.kernel.configuration.Config
import org.neo4j.logging.internal.LogService
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import streams.StreamsEventRouter
import streams.events.StreamsEvent
import streams.serialization.JSONUtils

class WalEventRouter(logService: LogService, config: Config) : StreamsEventRouter(logService, config) {
    private val log: Logger = LoggerFactory.getLogger("TransactionLog")

    override fun sendEvents(topic: String, transactionEvents: List<out StreamsEvent>) {
        transactionEvents.forEach {
            val event = JSONUtils.writeValueAsString(it)
            log.info(event)
        }
    }

    override fun start() {
    }

    override fun stop() {
    }


}