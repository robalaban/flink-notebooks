/**
 * Manages User-Defined Functions (UDFs) lifecycle
 */

import * as vscode from "vscode";
import * as path from "path";
import * as fs from "fs";
import { spawn } from "child_process";
import { SqlGatewayClient } from "./sqlGatewayClient";
import {
  getScalarFunctionTemplate,
  getScalarFunctionSqlRegistration,
} from "../templates/scalarFunctionTemplate";
import {
  getTableFunctionTemplate,
  getTableFunctionSqlRegistration,
} from "../templates/tableFunctionTemplate";
import {
  getAggregateFunctionTemplate,
  getAggregateFunctionSqlRegistration,
} from "../templates/aggregateFunctionTemplate";

export type UdfType = "scalar" | "table" | "aggregate";

export interface UdfInfo {
  className: string;
  functionName: string;
  type: UdfType;
  filePath: string;
  registered: boolean;
}

export interface Logger {
  log: (message: string) => void;
  error: (message: string) => void;
}

export class UdfManager {
  private registeredUdfs: Map<string, UdfInfo> = new Map();
  private logger?: Logger;
  private udfSourcePath?: string;
  private flinkRuntimePath?: string;
  private extensionPath?: string;

  constructor(extensionPath?: string) {
    // Store extension path for finding bundled build tools
    this.extensionPath = extensionPath;
  }

  /**
   * Get workspace root, throwing helpful error if not available
   */
  private getWorkspaceRoot(): string {
    const workspaceRoot = vscode.workspace.workspaceFolders?.[0]?.uri.fsPath;
    if (!workspaceRoot) {
      throw new Error(
        "No workspace folder found. Please open the Flink Notebooks project folder in VSCode.",
      );
    }
    return workspaceRoot;
  }

  /**
   * Find the project root by looking for flink-runtime directory.
   * Supports two modes:
   * 1. Development mode: flink-runtime exists in workspace (for extension developers)
   * 2. Installed mode: flink-runtime exists in extension directory (for users)
   */
  private findProjectRoot(startPath: string): string {
    // First, try to find flink-runtime in workspace (development mode)
    let currentPath = startPath;
    const maxDepth = 5; // Prevent infinite loops

    for (let i = 0; i < maxDepth; i++) {
      const flinkRuntimePath = path.join(currentPath, "flink-runtime");

      // Check if flink-runtime exists at this level
      if (
        fs.existsSync(flinkRuntimePath) &&
        fs.statSync(flinkRuntimePath).isDirectory()
      ) {
        // Verify it has the gradlew file
        const gradlewPath = path.join(flinkRuntimePath, "gradlew");
        if (fs.existsSync(gradlewPath)) {
          if (this.logger) {
            this.logger.log(
              `Found flink-runtime in workspace (development mode): ${currentPath}`,
            );
          }
          return currentPath;
        }
      }

      // Go up one directory
      const parentPath = path.dirname(currentPath);
      if (parentPath === currentPath) {
        // Reached filesystem root
        break;
      }
      currentPath = parentPath;
    }

    // If not found in workspace, check extension directory (installed mode)
    if (this.extensionPath) {
      const extensionFlinkRuntime = path.join(
        this.extensionPath,
        "flink-runtime",
      );
      const gradlewPath = path.join(extensionFlinkRuntime, "gradlew");

      if (fs.existsSync(extensionFlinkRuntime) && fs.existsSync(gradlewPath)) {
        if (this.logger) {
          this.logger.log(
            `Found flink-runtime in extension directory (installed mode): ${this.extensionPath}`,
          );
        }
        return this.extensionPath;
      }
    }

    throw new Error(
      `Could not find flink-runtime directory with Gradle build tools.\n` +
        `Searched in workspace: ${startPath}\n` +
        (this.extensionPath
          ? `Searched in extension: ${this.extensionPath}\n`
          : "") +
        `\nThis may indicate the extension was not packaged correctly. ` +
        `Please report this issue at https://github.com/anthropics/flink-notebooks/issues`,
    );
  }

  /**
   * Get or initialize UDF paths
   */
  private initializePaths(): void {
    if (this.flinkRuntimePath && this.udfSourcePath) {
      return; // Already initialized
    }

    const workspaceRoot = this.getWorkspaceRoot();

    if (this.logger) {
      this.logger.log(`Initializing UDF paths...`);
      this.logger.log(`Workspace root: ${workspaceRoot}`);
      this.logger.log(`Extension path: ${this.extensionPath || '(not set)'}`);
    }

    // Find the actual project root (handles case where vscode-extension is opened)
    const projectRoot = this.findProjectRoot(workspaceRoot);

    this.flinkRuntimePath = path.join(projectRoot, "flink-runtime");

    // User UDFs go in workspace root, not in extension directory
    // This keeps user code separate from extension code
    this.udfSourcePath = path.join(
      workspaceRoot,
      "udfs",
      "src",
      "main",
      "java",
    );

    if (this.logger) {
      this.logger.log(`Project root: ${projectRoot}`);
      this.logger.log(`Flink runtime path: ${this.flinkRuntimePath}`);
      this.logger.log(`Workspace UDF path: ${this.udfSourcePath}`);
    }
  }

  /**
   * Get UDF source path, initializing if needed
   */
  private getUdfSourcePath(): string {
    this.initializePaths();
    return this.udfSourcePath!;
  }

  /**
   * Get Flink runtime path, initializing if needed
   */
  private getFlinkRuntimePath(): string {
    this.initializePaths();
    return this.flinkRuntimePath!;
  }

  /**
   * Path where the built UDF jar lives. Used by ADD JAR and by the build output dir.
   */
  getUdfJarPath(): string {
    const workspaceRoot = this.getWorkspaceRoot();
    return path.join(workspaceRoot, ".flink-udfs", "flink-udfs.jar");
  }

  /**
   * Path to the workspace-local output directory the gradle build writes into.
   */
  private getUdfOutputDir(): string {
    const workspaceRoot = this.getWorkspaceRoot();
    return path.join(workspaceRoot, ".flink-udfs");
  }

  setLogger(logger: Logger): void {
    this.logger = logger;
  }

  /**
   * Create a new UDF file from template
   */
  async createUdf(
    className: string,
    functionName: string,
    type: UdfType,
    description?: string,
  ): Promise<string> {
    // Validate class name (must be valid Java identifier)
    if (!/^[A-Z][a-zA-Z0-9]*$/.test(className)) {
      throw new Error(
        "Class name must start with uppercase letter and contain only alphanumeric characters",
      );
    }

    // Validate function name (SQL identifier)
    if (!/^[a-z][a-z0-9_]*$/.test(functionName)) {
      throw new Error(
        "Function name must start with lowercase letter and contain only lowercase letters, numbers, and underscores",
      );
    }

    // Ensure UDF directory exists
    const udfSourcePath = this.getUdfSourcePath();
    if (!fs.existsSync(udfSourcePath)) {
      fs.mkdirSync(udfSourcePath, { recursive: true });
    }

    // Generate template code
    const templateCode = this.getTemplate(
      className,
      functionName,
      type,
      description || "",
    );

    // Write to file
    const filePath = path.join(udfSourcePath, `${className}.java`);
    if (fs.existsSync(filePath)) {
      throw new Error(`UDF class ${className} already exists at ${filePath}`);
    }

    fs.writeFileSync(filePath, templateCode, "utf-8");

    // Track UDF
    const udfInfo: UdfInfo = {
      className,
      functionName,
      type,
      filePath,
      registered: false,
    };
    this.registeredUdfs.set(functionName, udfInfo);

    if (this.logger) {
      this.logger.log(`Created UDF ${className} at ${filePath}`);
    }

    return filePath;
  }

  /**
   * Build all UDFs into JAR using Gradle
   */
  async buildUdfs(): Promise<void> {
    if (this.logger) {
      this.logger.log("Building UDFs...");
    }

    return new Promise((resolve, reject) => {
      const flinkRuntimePath = this.getFlinkRuntimePath();
      const workspaceRoot = this.getWorkspaceRoot();
      const gradlew =
        process.platform === "win32" ? "gradlew.bat" : "./gradlew";
      const gradlewPath = path.join(flinkRuntimePath, gradlew);

      // Pass workspace UDF directory to Gradle
      const workspaceUdfDir = path.join(workspaceRoot, "udfs");

      const buildProcess = spawn(
        gradlewPath,
        [":udfs:shadowJar", `-PworkspaceUdfDir=${workspaceUdfDir}`],
        {
          cwd: flinkRuntimePath,
          shell: true,
        },
      );

      let stdout = "";
      let stderr = "";

      buildProcess.stdout?.on("data", (data) => {
        const output = data.toString();
        stdout += output;
        if (this.logger) {
          this.logger.log(`[Gradle] ${output.trim()}`);
        }
      });

      buildProcess.stderr?.on("data", (data) => {
        const output = data.toString();
        stderr += output;
        if (this.logger) {
          this.logger.error(`[Gradle] ${output.trim()}`);
        }
      });

      buildProcess.on("close", (code) => {
        if (code === 0) {
          if (this.logger) {
            this.logger.log("UDFs built successfully");
          }
          resolve();
        } else {
          const error = new Error(
            `Gradle build failed with exit code ${code}\n${stderr}`,
          );
          if (this.logger) {
            this.logger.error(`Build failed: ${error.message}`);
          }
          reject(error);
        }
      });

      buildProcess.on("error", (error) => {
        if (this.logger) {
          this.logger.error(`Failed to start Gradle: ${error.message}`);
        }
        reject(error);
      });
    });
  }

  /**
   * Register a UDF in the current Flink session
   */
  async registerUdf(
    gatewayClient: SqlGatewayClient,
    sessionHandle: string,
    functionName: string,
  ): Promise<void> {
    const udfInfo = this.registeredUdfs.get(functionName);
    if (!udfInfo) {
      throw new Error(`UDF ${functionName} not found in registry`);
    }

    const registrationSql = this.getSqlRegistration(
      functionName,
      udfInfo.className,
      udfInfo.type,
    );

    if (this.logger) {
      this.logger.log(`Registering UDF ${functionName}: ${registrationSql}`);
    }

    try {
      const result = await gatewayClient.executeStatement(
        sessionHandle,
        registrationSql,
      );

      // Wait for operation to complete
      let status = await gatewayClient.getStatementInfo(
        sessionHandle,
        result.operationHandle,
      );

      // Poll until operation completes
      const maxAttempts = 10;
      let attempts = 0;
      while (
        status.status !== "FINISHED" &&
        status.status !== "ERROR" &&
        attempts < maxAttempts
      ) {
        await new Promise((resolve) => setTimeout(resolve, 500));
        status = await gatewayClient.getStatementInfo(
          sessionHandle,
          result.operationHandle,
        );
        attempts++;
      }

      if (status.status === "ERROR") {
        throw new Error(`Failed to register UDF ${functionName}`);
      }

      udfInfo.registered = true;

      if (this.logger) {
        this.logger.log(`Successfully registered UDF ${functionName}`);
      }
    } catch (error) {
      if (this.logger) {
        this.logger.error(
          `Failed to register UDF ${functionName}: ${error instanceof Error ? error.message : String(error)}`,
        );
      }
      throw error;
    }
  }

  /**
   * Issue ADD JAR for the workspace UDF jar so the session classloader picks it up.
   * Silently skips if the jar doesn't exist yet (user hasn't built).
   */
  async addJarToSession(
    gatewayClient: SqlGatewayClient,
    sessionHandle: string,
  ): Promise<void> {
    const jarPath = this.getUdfJarPath();

    if (!fs.existsSync(jarPath)) {
      if (this.logger) {
        this.logger.log(
          `No UDF jar at ${jarPath} - run "Flink: Build UDFs" to enable UDFs.`,
        );
      }
      return;
    }

    const sql = `ADD JAR '${jarPath}'`;
    if (this.logger) {
      this.logger.log(`Loading UDF jar into session: ${sql}`);
    }

    const result = await gatewayClient.executeStatement(sessionHandle, sql);

    let status = await gatewayClient.getStatementInfo(
      sessionHandle,
      result.operationHandle,
    );
    const maxAttempts = 10;
    let attempts = 0;
    while (
      status.status !== "FINISHED" &&
      status.status !== "ERROR" &&
      attempts < maxAttempts
    ) {
      await new Promise((resolve) => setTimeout(resolve, 500));
      status = await gatewayClient.getStatementInfo(
        sessionHandle,
        result.operationHandle,
      );
      attempts++;
    }

    if (status.status !== "FINISHED") {
      throw new Error(`ADD JAR failed for ${jarPath} (status=${status.status})`);
    }
  }

  /**
   * Register all tracked UDFs in the session. Loads the jar first via ADD JAR;
   * if the jar load fails, skip the per-UDF registration loop entirely (they'd all fail).
   */
  async registerAllUdfs(
    gatewayClient: SqlGatewayClient,
    sessionHandle: string,
  ): Promise<void> {
    const udfs = Array.from(this.registeredUdfs.values());

    if (udfs.length === 0) {
      if (this.logger) {
        this.logger.log("No UDFs to register");
      }
      // Still try to load the jar in case bundled examples exist.
    }

    try {
      await this.addJarToSession(gatewayClient, sessionHandle);
    } catch (error) {
      if (this.logger) {
        this.logger.error(
          `Failed to ADD JAR; skipping UDF registration: ${error instanceof Error ? error.message : String(error)}`,
        );
      }
      throw error;
    }

    if (udfs.length === 0) {
      return;
    }

    if (this.logger) {
      this.logger.log(`Registering ${udfs.length} UDF(s)...`);
    }

    for (const udf of udfs) {
      try {
        await this.registerUdf(gatewayClient, sessionHandle, udf.functionName);
      } catch (error) {
        if (this.logger) {
          this.logger.error(
            `Failed to register ${udf.functionName}: ${error instanceof Error ? error.message : String(error)}`,
          );
        }
      }
    }
  }

  /**
   * Scan UDF directory and populate registry
   */
  async scanUdfs(): Promise<UdfInfo[]> {
    // Check if workspace is available
    try {
      this.initializePaths();
    } catch (error) {
      // No workspace folder - return empty array
      if (this.logger) {
        this.logger.log("No workspace folder - skipping UDF scan");
      }
      return [];
    }

    const udfSourcePath = this.getUdfSourcePath();
    if (!fs.existsSync(udfSourcePath)) {
      return [];
    }

    const javaFiles = fs
      .readdirSync(udfSourcePath)
      .filter((file) => file.endsWith(".java"));

    const udfs: UdfInfo[] = [];

    for (const file of javaFiles) {
      const filePath = path.join(udfSourcePath, file);
      const className = file.replace(".java", "");

      // Read file to extract function name and type from comments
      const content = fs.readFileSync(filePath, "utf-8");

      // Parse CREATE TEMPORARY FUNCTION line to get function name
      const functionMatch = content.match(
        /CREATE\s+TEMPORARY\s+FUNCTION\s+(\w+)/i,
      );
      const functionName = functionMatch
        ? functionMatch[1]
        : className.toLowerCase();

      // Determine type from extends clause
      let type: UdfType = "scalar";
      if (content.includes("extends TableFunction")) {
        type = "table";
      } else if (content.includes("extends AggregateFunction")) {
        type = "aggregate";
      }

      const udfInfo: UdfInfo = {
        className,
        functionName,
        type,
        filePath,
        registered: false,
      };

      udfs.push(udfInfo);
      this.registeredUdfs.set(functionName, udfInfo);
    }

    if (this.logger) {
      this.logger.log(`Scanned ${udfs.length} UDF(s) from ${udfSourcePath}`);
    }

    return udfs;
  }

  /**
   * Get all registered UDFs
   */
  getRegisteredUdfs(): UdfInfo[] {
    return Array.from(this.registeredUdfs.values());
  }

  /**
   * Clear registered status (e.g., when session is closed)
   */
  clearRegisteredStatus(): void {
    for (const udf of this.registeredUdfs.values()) {
      udf.registered = false;
    }
  }

  private getTemplate(
    className: string,
    functionName: string,
    type: UdfType,
    description: string,
  ): string {
    switch (type) {
      case "scalar":
        return getScalarFunctionTemplate(className, functionName, description);
      case "table":
        return getTableFunctionTemplate(className, functionName, description);
      case "aggregate":
        return getAggregateFunctionTemplate(
          className,
          functionName,
          description,
        );
      default:
        throw new Error(`Unknown UDF type: ${type}`);
    }
  }

  private getSqlRegistration(
    functionName: string,
    className: string,
    type: UdfType,
  ): string {
    switch (type) {
      case "scalar":
        return getScalarFunctionSqlRegistration(functionName, className);
      case "table":
        return getTableFunctionSqlRegistration(functionName, className);
      case "aggregate":
        return getAggregateFunctionSqlRegistration(functionName, className);
      default:
        throw new Error(`Unknown UDF type: ${type}`);
    }
  }
}
