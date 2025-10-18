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
}

export class ClusterManager {
  private process: ChildProcess | null = null;
  private status: ClusterStatus = ClusterStatus.STOPPED;
  private pid: number | null = null;
  private config: Required<ClusterConfig>;

  constructor(config: ClusterConfig = {}) {
    this.config = {
      javaPath: config.javaPath || this.findJava(),
      jvmMemory: config.jvmMemory || '1024m',
      parallelism: config.parallelism || 2,
      taskSlots: config.taskSlots || 2,
      gatewayPort: config.gatewayPort || 8083,
      jarPath: config.jarPath || this.findJarPath(),
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

    const args = [
      `-Xmx${memoryArg}`,
      '-jar',
      this.config.jarPath,
      '--parallelism',
      String(parallelismArg),
      '--taskslots',
      String(this.config.taskSlots),
      '--gateway-port',
      String(this.config.gatewayPort),
    ];

    const env = {
      ...process.env,
      FLINK_CONF_DIR: flinkConfDir,
    };

    console.log(`Starting MiniCluster: ${this.config.javaPath} ${args.join(' ')}`);
    console.log(`FLINK_CONF_DIR: ${flinkConfDir}`);

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
          console.log(`[MiniCluster] ${data.toString().trim()}`);
        });
      }

      // Capture and log stderr
      if (this.process.stderr) {
        this.process.stderr.on('data', (data) => {
          console.error(`[MiniCluster] ${data.toString().trim()}`);
        });
      }

      this.process.on('error', (error) => {
        console.error(`MiniCluster process error: ${error.message}`);
        this.status = ClusterStatus.ERROR;
      });

      this.process.on('exit', (code, signal) => {
        console.log(`MiniCluster process exited with code ${code}, signal ${signal}`);
        this.status = ClusterStatus.STOPPED;
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

    throw new Error(
      `SQL Gateway did not become ready within ${timeout / 1000} seconds. ` +
      `Process is still running (PID: ${this.pid}). Check console logs for details.`
    );
  }
}
