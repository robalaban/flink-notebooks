/**
 * Manages the Flink MiniCluster lifecycle
 */

import { spawn, ChildProcess } from 'child_process';
import * as path from 'path';
import * as fs from 'fs';
import axios from 'axios';

export enum ClusterStatus {
  STOPPED = 'stopped',
  STARTING = 'starting',
  RUNNING = 'running',
  STOPPING = 'stopping',
  ERROR = 'error',
}

export interface ClusterConfig {
  javaPath?: string;
  jvmMemory?: string;
  parallelism?: number;
  taskSlots?: number;
  gatewayPort?: number;
  jarPath?: string;
  connectorLibraryPath?: string;
}

export interface Logger {
  log: (message: string) => void;
  error: (message: string) => void;
}

export class ClusterManager {
  private process: ChildProcess | null = null;
  private status: ClusterStatus = ClusterStatus.STOPPED;
  private pid: number | null = null;
  private config: Required<ClusterConfig>;
  private crashHandlers: Array<(exitCode: number | null) => void> = [];
  private logger?: Logger;

  constructor(config: ClusterConfig = {}) {
    this.config = {
      javaPath: config.javaPath || this.findJava(),
      jvmMemory: config.jvmMemory || '1024m',
      parallelism: config.parallelism || 2,
      taskSlots: config.taskSlots || 2,
      gatewayPort: config.gatewayPort || 8083,
      jarPath: config.jarPath || this.findJarPath(),
      connectorLibraryPath: config.connectorLibraryPath || '',
    };
  }

  private findJava(): string {
    // Use JAVA_HOME if available, otherwise rely on PATH
    const javaHome = process.env.JAVA_HOME;
    if (javaHome) {
      const javaBin = path.join(javaHome, 'bin', 'java');
      if (fs.existsSync(javaBin)) {
        return javaBin;
      }
    }
    return 'java'; // Rely on PATH
  }

  private findJarPath(): string {
    // Extension is at: vscode-extension/
    // JAR is at: flink-runtime/build/libs/flink-minicluster.jar
    const extensionDir = path.join(__dirname, '..', '..'); // From out/services/ to vscode-extension/
    const jarPath = path.join(
      extensionDir,
      '..',
      'flink-runtime',
      'build',
      'libs',
      'flink-minicluster.jar'
    );
    return path.resolve(jarPath);
  }

  private getFlinkConfDir(): string {
    // From JAR path, go up to flink-runtime root, then to conf/
    const jarPath = this.config.jarPath;
    const flinkRuntimeRoot = path.dirname(path.dirname(path.dirname(jarPath)));
    return path.join(flinkRuntimeRoot, 'conf');
  }

  private getConnectorLibDir(): string {
    // If user specified a custom path, use it
    if (this.config.connectorLibraryPath) {
      return path.resolve(this.config.connectorLibraryPath);
    }

    // Otherwise, use default lib/ directory next to flink-runtime/conf
    const jarPath = this.config.jarPath;
    const flinkRuntimeRoot = path.dirname(path.dirname(path.dirname(jarPath)));
    return path.join(flinkRuntimeRoot, 'lib');
  }

  private scanConnectorJars(libDir: string): string[] {
    try {
      if (!fs.existsSync(libDir) || !fs.statSync(libDir).isDirectory()) {
        return [];
      }

      const files = fs.readdirSync(libDir);
      const jars = files
        .filter((file) => file.toLowerCase().endsWith('.jar'))
        .map((file) => path.join(libDir, file));

      if (jars.length > 0 && this.logger) {
        this.logger.log(`Found ${jars.length} connector JAR(s) in ${libDir}`);
        jars.forEach((jar) => this.logger!.log(`  - ${path.basename(jar)}`));
      }

      return jars;
    } catch (error) {
      console.error(`Error scanning connector JARs in ${libDir}:`, error);
      return [];
    }
  }

  getStatus(): ClusterStatus {
    // Check if process is still alive
    if (this.process && this.process.exitCode !== null) {
      this.status = ClusterStatus.STOPPED;
      this.process = null;
      this.pid = null;
    }
    return this.status;
  }

  getPid(): number | null {
    return this.pid;
  }

  getGatewayUrl(): string {
    return `http://localhost:${this.config.gatewayPort}`;
  }

  /**
   * Set a logger to receive cluster log messages
   */
  setLogger(logger: Logger): void {
    this.logger = logger;
  }

  /**
   * Register a callback to be notified when the cluster crashes unexpectedly
   */
  onCrash(handler: (exitCode: number | null) => void): void {
    this.crashHandlers.push(handler);
  }

  /**
   * Notify all registered crash handlers
   */
  private notifyCrashHandlers(exitCode: number | null): void {
    for (const handler of this.crashHandlers) {
      try {
        handler(exitCode);
      } catch (error) {
        console.error('Error in crash handler:', error);
      }
    }
  }

  async start(memory?: string, parallelism?: number): Promise<void> {
    if (this.status === ClusterStatus.RUNNING || this.status === ClusterStatus.STARTING) {
      throw new Error('Cluster is already running or starting');
    }

    this.status = ClusterStatus.STARTING;

    // Check JAR exists
    if (!fs.existsSync(this.config.jarPath)) {
      this.status = ClusterStatus.ERROR;
      throw new Error(
        `MiniCluster JAR not found at ${this.config.jarPath}. ` +
        `Please build it first: cd flink-runtime && ./gradlew shadowJar`
      );
    }

    // Check Flink config dir exists
    const flinkConfDir = this.getFlinkConfDir();
    if (!fs.existsSync(flinkConfDir)) {
      this.status = ClusterStatus.ERROR;
      throw new Error(
        `Flink configuration directory not found: ${flinkConfDir}. ` +
        `Please create ${path.join(flinkConfDir, 'flink-conf.yaml')}`
      );
    }

    const memoryArg = memory || this.config.jvmMemory;
    const parallelismArg = parallelism || this.config.parallelism;

    // Get connector library directory and scan for JARs
    const connectorLibDir = this.getConnectorLibDir();
    const connectorJars = this.scanConnectorJars(connectorLibDir);

    // Build classpath: main JAR + connector JARs
    const classpath = [this.config.jarPath, ...connectorJars].join(path.delimiter);

    const args = [
      `-Xmx${memoryArg}`,
      '-cp',
      classpath,
      'com.flink.notebooks.MiniClusterRunner',
      '--parallelism',
      String(parallelismArg),
      '--taskslots',
      String(this.config.taskSlots),
      '--gateway-port',
      String(this.config.gatewayPort),
    ];

    const env: NodeJS.ProcessEnv = {
      ...process.env,
      FLINK_CONF_DIR: flinkConfDir,
    };

    // Add FLINK_CONNECTOR_LIB_DIR if a custom path is configured
    if (this.config.connectorLibraryPath) {
      env.FLINK_CONNECTOR_LIB_DIR = connectorLibDir;
    }

    console.log(`Starting MiniCluster: ${this.config.javaPath} -cp <classpath> com.flink.notebooks.MiniClusterRunner ...`);
    console.log(`FLINK_CONF_DIR: ${flinkConfDir}`);
    console.log(`Connector JARs: ${connectorJars.length} JAR(s) added to classpath`);
    if (this.logger) {
      this.logger.log(`Starting MiniCluster with ${connectorJars.length} connector JAR(s) in classpath`);
      this.logger.log(`Connector library directory: ${connectorLibDir}`);
    }

    try {
      this.process = spawn(this.config.javaPath, args, {
        env,
        stdio: ['ignore', 'pipe', 'pipe'], // Capture stdout and stderr
        detached: false,
      });

      this.pid = this.process.pid || null;

      // Capture and log stdout
      if (this.process.stdout) {
        this.process.stdout.on('data', (data) => {
          const message = `[MiniCluster] ${data.toString().trim()}`;
          console.log(message);
          if (this.logger) {
            this.logger.log(message);
          }
        });
      }

      // Capture and log stderr
      if (this.process.stderr) {
        this.process.stderr.on('data', (data) => {
          const message = `[MiniCluster] ${data.toString().trim()}`;
          console.error(message);
          if (this.logger) {
            this.logger.error(message);
          }
        });
      }

      this.process.on('error', (error) => {
        const message = `MiniCluster process error: ${error.message}`;
        console.error(message);
        if (this.logger) {
          this.logger.error(message);
        }
        this.status = ClusterStatus.ERROR;
        // Notify crash handlers
        this.notifyCrashHandlers(null);
      });

      this.process.on('exit', (code, signal) => {
        const wasRunning = this.status === ClusterStatus.RUNNING;
        const wasStopping = this.status === ClusterStatus.STOPPING;
        const exitMessage = `MiniCluster process exited with code ${code}, signal ${signal}`;
        console.log(exitMessage);
        if (this.logger) {
          this.logger.log(exitMessage);
        }

        // Detect unexpected crash (not during normal stop/start)
        // Crash if: was running AND (non-zero exit OR killed by signal)
        const wasUnexpected = wasRunning && !wasStopping && (code !== 0 || signal !== null);

        if (wasUnexpected) {
          const crashMessage = `MiniCluster crashed unexpectedly with exit code ${code}, signal ${signal}`;
          console.error(crashMessage);
          if (this.logger) {
            this.logger.error(crashMessage);
          }
          this.status = ClusterStatus.ERROR;
          // Notify crash handlers of unexpected crash
          this.notifyCrashHandlers(code);
        } else {
          this.status = ClusterStatus.STOPPED;
        }

        this.process = null;
        this.pid = null;
      });

      console.log(`MiniCluster started with PID ${this.pid}`);

      // Wait for gateway to be ready
      await this.waitForGateway();

      this.status = ClusterStatus.RUNNING;
      console.log('MiniCluster is ready');
    } catch (error) {
      this.status = ClusterStatus.ERROR;
      throw error;
    }
  }

  async stop(): Promise<void> {
    if (this.status === ClusterStatus.STOPPED) {
      console.log('Cluster is already stopped');
      return;
    }

    this.status = ClusterStatus.STOPPING;
    console.log('Stopping MiniCluster');

    if (this.process) {
      try {
        // Try graceful shutdown first
        this.process.kill('SIGTERM');

        // Wait up to 10 seconds for graceful shutdown
        await new Promise<void>((resolve) => {
          const timeout = setTimeout(() => {
            if (this.process && this.process.exitCode === null) {
              console.log('Graceful shutdown timed out, forcing kill');
              this.process.kill('SIGKILL');
            }
            resolve();
          }, 10000);

          if (this.process) {
            this.process.once('exit', () => {
              clearTimeout(timeout);
              resolve();
            });
          } else {
            clearTimeout(timeout);
            resolve();
          }
        });

        console.log('MiniCluster stopped');
      } catch (error) {
        console.error(`Error stopping MiniCluster: ${error}`);
      } finally {
        this.process = null;
        this.pid = null;
        this.status = ClusterStatus.STOPPED;
      }
    }
  }

  private async waitForGateway(timeout: number = 30000): Promise<void> {
    const gatewayUrl = `${this.getGatewayUrl()}/v1/info`;
    console.log(`Waiting for SQL Gateway at ${gatewayUrl}`);

    const startTime = Date.now();

    while (Date.now() - startTime < timeout) {
      // Check if process is still alive
      if (this.process && this.process.exitCode !== null) {
        throw new Error(
          `MiniCluster process terminated unexpectedly with exit code ${this.process.exitCode}. ` +
          `Check the console output for error details.`
        );
      }

      try {
        const response = await axios.get(gatewayUrl, { timeout: 2000 });
        if (response.status === 200) {
          console.log('SQL Gateway is ready');
          return;
        }
      } catch (error) {
        // Gateway not ready yet, continue polling
      }

      // Wait 1 second before next attempt
      await new Promise((resolve) => setTimeout(resolve, 1000));
    }

    // Gateway failed to start - kill the process to prevent orphan
    console.error(
      `SQL Gateway did not become ready within ${timeout / 1000} seconds. ` +
      `Killing process (PID: ${this.pid}) to prevent orphan.`
    );

    try {
      await this.stop();
    } catch (stopError) {
      console.error('Error killing process after startup timeout:', stopError);
    }

    throw new Error(
      `SQL Gateway did not become ready within ${timeout / 1000} seconds. ` +
      `The Java process has been terminated. Check console logs for details. ` +
      `Common causes: port conflicts (8081, 8083, 6123), insufficient memory, or missing Java dependencies.`
    );
  }
}
