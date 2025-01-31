package streams

import org.neo4j.kernel.availability.AvailabilityGuard
import org.neo4j.kernel.availability.AvailabilityListener
import org.neo4j.kernel.configuration.Config
import org.neo4j.kernel.extension.ExtensionType
import org.neo4j.kernel.extension.KernelExtensionFactory
import org.neo4j.kernel.impl.spi.KernelContext
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.kernel.lifecycle.Lifecycle
import org.neo4j.kernel.lifecycle.LifecycleAdapter
import org.neo4j.logging.internal.LogService
import streams.procedures.StreamsProcedures

class StreamsExtensionFactory : KernelExtensionFactory<StreamsExtensionFactory.Dependencies>(ExtensionType.DATABASE,"Streams.Producer") {
    override fun newInstance(context: KernelContext, dependencies: Dependencies): Lifecycle {
        val db = dependencies.graphdatabaseAPI()
        val log = dependencies.log()
        val configuration = dependencies.config()
        val streamHandler = StreamsEventRouterFactory.getStreamsEventRouter(log, configuration)
        val streamsEventRouterConfiguration = StreamsEventRouterConfiguration.from(configuration.raw)
        return StreamsEventRouterLifecycle(db, streamHandler, streamsEventRouterConfiguration, dependencies.availabilityGuard(), log)
    }

    interface Dependencies {
        fun graphdatabaseAPI(): GraphDatabaseAPI
        fun log(): LogService
        fun config(): Config
        fun availabilityGuard(): AvailabilityGuard
    }
}

class StreamsEventRouterLifecycle(val db: GraphDatabaseAPI, val streamHandler: StreamsEventRouter,
                                  val streamsEventRouterConfiguration: StreamsEventRouterConfiguration,
                                  private val availabilityGuard: AvailabilityGuard,
                                  private val log: LogService): LifecycleAdapter() {
    private val streamsLog = log.getUserLog(StreamsEventRouterLifecycle::class.java)
    private lateinit var txHandler: StreamsTransactionEventHandler
    private lateinit var streamsConstraintsService: StreamsConstraintsService

    override fun start() {
        try {
            streamsLog.info("Initialising the Streams Source module")
            StreamsProcedures.registerEventRouter(eventRouter = streamHandler)
            StreamsProcedures.registerEventRouterConfiguration(eventRouterConfiguration = streamsEventRouterConfiguration)
            streamHandler.start()
            registerTransactionEventHandler()
            streamsLog.info("Streams Source module initialised")
        } catch (e: Exception) {
            e.printStackTrace()
            streamsLog.error("Error initializing the streaming producer", e)
        }
    }

    private fun registerTransactionEventHandler() {
        if (streamsEventRouterConfiguration.enabled) {
            streamsConstraintsService = StreamsConstraintsService(db, streamsEventRouterConfiguration.schemaPollingInterval)
            txHandler = StreamsTransactionEventHandler(streamHandler, streamsConstraintsService, streamsEventRouterConfiguration, streamsLog)
            db.registerTransactionEventHandler(txHandler)
            availabilityGuard.addListener(object: AvailabilityListener {
                override fun unavailable() {}

                override fun available() {
                    streamsConstraintsService.start()
                }
            })
        }
    }

    private fun unregisterTransactionEventHandler() {
        if (streamsEventRouterConfiguration.enabled) {
            streamsConstraintsService.close()
            db.unregisterTransactionEventHandler(txHandler)
        }
    }

    override fun stop() {
        unregisterTransactionEventHandler()
        streamHandler.stop()
    }
}
