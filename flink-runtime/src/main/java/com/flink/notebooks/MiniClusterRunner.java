package com.flink.notebooks;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.table.gateway.rest.SqlGatewayRestEndpoint;
import org.apache.flink.table.gateway.rest.SqlGatewayRestEndpointFactory;
import org.apache.flink.table.gateway.service.SqlGatewayServiceImpl;
import org.apache.flink.table.gateway.service.context.DefaultContext;
import org.apache.flink.table.gateway.service.session.SessionManagerImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Runner for Flink MiniCluster with embedded SQL Gateway.
 *
 * This provides a lightweight local Flink environment for development and testing.
 */
public class MiniClusterRunner {

    private static final Logger LOG = LoggerFactory.getLogger(MiniClusterRunner.class);

    private static final int DEFAULT_PARALLELISM = 2;
    private static final int DEFAULT_TASK_SLOTS = 2;
    private static final int DEFAULT_GATEWAY_PORT = 8083;

    private MiniCluster miniCluster;
    private SqlGatewayRestEndpoint gateway;
    private ExecutorService operationExecutor;
    private ExecutorService cleanupExecutor;

    public static void main(String[] args) throws Exception {
        MiniClusterRunner runner = new MiniClusterRunner();

        // Parse command line arguments
        Config config = parseArgs(args);

        LOG.info("Starting Flink MiniCluster Runner");
        LOG.info("  Parallelism: {}", config.parallelism);
        LOG.info("  Task Slots: {}", config.taskSlots);
        LOG.info("  Gateway Port: {}", config.gatewayPort);

        try {
            runner.start(config);

            // Keep the application running
            CountDownLatch latch = new CountDownLatch(1);

            // Shutdown hook for graceful termination
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                LOG.info("Shutdown signal received");
                try {
                    runner.stop();
                } catch (Exception e) {
                    LOG.error("Error during shutdown", e);
                }
                latch.countDown();
            }));

            LOG.info("MiniCluster is running. Press Ctrl+C to stop.");
            latch.await();

        } catch (Exception e) {
            LOG.error("Failed to start MiniCluster", e);
            System.exit(1);
        }
    }

    public void start(Config config) throws Exception {
        // Load configuration from flink-conf.yaml
        // This is important to pick up security.delegation.tokens.enabled=false
        Configuration flinkConfig = org.apache.flink.configuration.GlobalConfiguration.loadConfiguration();

        // Override REST port
        flinkConfig.set(RestOptions.PORT, 8081);

        MiniClusterConfiguration miniClusterConfig = new MiniClusterConfiguration.Builder()
            .setConfiguration(flinkConfig)
            .setNumTaskManagers(1)
            .setNumSlotsPerTaskManager(config.taskSlots)
            .build();

        // Start MiniCluster
        LOG.info("Starting Flink MiniCluster...");
        miniCluster = new MiniCluster(miniClusterConfig);
        miniCluster.start();
        LOG.info("MiniCluster started successfully");

        // Configure and start SQL Gateway
        LOG.info("Starting SQL Gateway on port {}...", config.gatewayPort);

        // Configure for connecting to the already-running MiniCluster
        // Use 'remote' target pointing to MiniCluster's REST endpoint at localhost:8081
        Configuration sessionConfig = new Configuration();
        sessionConfig.set(org.apache.flink.configuration.DeploymentOptions.TARGET, "remote");
        sessionConfig.setString("jobmanager.rpc.address", "localhost");
        sessionConfig.setInteger("jobmanager.rpc.port", 6123);
        sessionConfig.setString("rest.address", "localhost");
        sessionConfig.setInteger("rest.port", 8081);

        // Configuration for SQL Gateway's own REST endpoint (port 8083)
        Configuration gatewayConfig = new Configuration();
        gatewayConfig.setInteger("rest.port", config.gatewayPort);
        gatewayConfig.setString("rest.address", "0.0.0.0");
        gatewayConfig.setString("rest.bind-address", "0.0.0.0");

        // Create executor service for SQL Gateway operations
        operationExecutor = Executors.newFixedThreadPool(
            10,
            r -> {
                Thread thread = new Thread(r);
                thread.setName("sql-gateway-operation-" + thread.getId());
                thread.setDaemon(true);
                return thread;
            }
        );

        // Load DefaultContext with the session configuration (for connecting to MiniCluster)
        DefaultContext defaultContext = DefaultContext.load(sessionConfig, Collections.emptyList(), true);

        SessionManagerImpl sessionManager = new SessionManagerImpl(defaultContext);

        SqlGatewayServiceImpl gatewayService = new SqlGatewayServiceImpl(sessionManager);

        // Set the executor service in SessionManagerImpl via reflection
        try {
            java.lang.reflect.Field operationExecutorField = SessionManagerImpl.class.getDeclaredField("operationExecutorService");
            operationExecutorField.setAccessible(true);
            operationExecutorField.set(sessionManager, operationExecutor);

            cleanupExecutor = Executors.newScheduledThreadPool(1, r -> {
                Thread thread = new Thread(r);
                thread.setName("sql-gateway-cleanup");
                thread.setDaemon(true);
                return thread;
            });
            java.lang.reflect.Field cleanupServiceField = SessionManagerImpl.class.getDeclaredField("cleanupService");
            cleanupServiceField.setAccessible(true);
            cleanupServiceField.set(sessionManager, cleanupExecutor);

            LOG.info("Successfully initialized operation and cleanup executors via reflection");
        } catch (Exception e) {
            LOG.error("Failed to set executors via reflection: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to initialize SQL Gateway executors", e);
        }

        gateway = new SqlGatewayRestEndpoint(gatewayConfig, gatewayService);
        gateway.start();

        LOG.info("SQL Gateway started successfully on http://localhost:{}", config.gatewayPort);
        LOG.info("Flink Web UI available at http://localhost:8081");
    }

    public void stop() throws Exception {
        LOG.info("Stopping MiniCluster...");

        if (gateway != null) {
            try {
                gateway.close();
                LOG.info("SQL Gateway stopped");
            } catch (Exception e) {
                LOG.error("Error stopping SQL Gateway", e);
            }
        }

        if (operationExecutor != null) {
            try {
                operationExecutor.shutdownNow();
                LOG.info("Operation executor stopped");
            } catch (Exception e) {
                LOG.error("Error stopping operation executor", e);
            }
        }

        if (cleanupExecutor != null) {
            try {
                cleanupExecutor.shutdownNow();
                LOG.info("Cleanup executor stopped");
            } catch (Exception e) {
                LOG.error("Error stopping cleanup executor", e);
            }
        }

        if (miniCluster != null) {
            try {
                miniCluster.close();
                LOG.info("MiniCluster stopped");
            } catch (Exception e) {
                LOG.error("Error stopping MiniCluster", e);
            }
        }
    }

    private static Config parseArgs(String[] args) {
        Config config = new Config();

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--parallelism":
                    if (i + 1 < args.length) {
                        config.parallelism = Integer.parseInt(args[++i]);
                    }
                    break;
                case "--taskslots":
                    if (i + 1 < args.length) {
                        config.taskSlots = Integer.parseInt(args[++i]);
                    }
                    break;
                case "--gateway-port":
                    if (i + 1 < args.length) {
                        config.gatewayPort = Integer.parseInt(args[++i]);
                    }
                    break;
                case "--help":
                    printHelp();
                    System.exit(0);
                    break;
            }
        }

        return config;
    }

    private static void printHelp() {
        System.out.println("Flink MiniCluster Runner");
        System.out.println();
        System.out.println("Usage: java -jar flink-minicluster.jar [options]");
        System.out.println();
        System.out.println("Options:");
        System.out.println("  --parallelism <num>     Set parallelism level (default: 2)");
        System.out.println("  --taskslots <num>       Set task slots per TaskManager (default: 2)");
        System.out.println("  --gateway-port <port>   Set SQL Gateway port (default: 8083)");
        System.out.println("  --help                  Show this help message");
    }

    private static class Config {
        int parallelism = DEFAULT_PARALLELISM;
        int taskSlots = DEFAULT_TASK_SLOTS;
        int gatewayPort = DEFAULT_GATEWAY_PORT;
    }
}
