# Dynamic UDF Loading Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Eliminate the cluster-restart requirement when adding/changing UDFs by moving UDF loading from the cluster's static `lib/` classpath to per-session `ADD JAR` + `CREATE TEMPORARY FUNCTION`, recycling the session on rebuild.

**Architecture:** UDFs build to a workspace-local jar (`<ws>/.flink-udfs/flink-udfs.jar`) instead of `flink-runtime/lib/`. On session creation, the extension issues `ADD JAR` then `CREATE TEMPORARY FUNCTION` per UDF. On `Build UDFs`, the session is closed and reopened (~ms) instead of restarting the cluster (~30s). Streaming jobs survive the recycle because they run on the cluster, not in the session.

**Tech Stack:** TypeScript (VSCode extension), Java 17, Gradle 8 with Shadow plugin, Flink 1.20 SQL Gateway REST API.

---

## Notes

- **No test infrastructure exists in this project.** Per the design spec, this refactor does not introduce automated tests. Each task ends with a manual smoke test plus a commit. The `npm run compile` step is the one always-available correctness check for TypeScript code.
- **Setting naming:** the spec called the auto-rebuild flag `flink-notebooks.udfAutoRebuild`, but `package.json` already declares `flink-notebooks.udfAutoBuild` (currently unwired). To avoid duplicate settings, this plan reuses `udfAutoBuild`. The default flips from `true` (current declared default) to `false` (per spec).

---

## File Map

**Modified:**
- `flink-runtime/udfs/build.gradle` — accept `udfSourceDirs` (CSV) and `udfOutputDir` properties.
- `vscode-extension/package.json` — add `flink-notebooks.udfSourceDirs`; change `udfAutoBuild` default to `false`.
- `vscode-extension/src/services/udfManager.ts` — multi-dir scan, jar path helper, `ADD JAR` helper, new gradle property names.
- `vscode-extension/src/providers/flinkNotebookController.ts` — add `SessionManager.recycleSession()`.
- `vscode-extension/src/extension.ts` — wire `ADD JAR` into session-created callback, recycle session after build, add file watcher gated by `udfAutoBuild`, migrate stale `lib/flink-udfs.jar`.
- `.gitignore` — add `.flink-udfs/`.

**Created:** none.

---

## Task 1: Update gradle build to accept multiple source dirs and configurable output dir

**Files:**
- Modify: `flink-runtime/udfs/build.gradle`

- [ ] **Step 1: Replace the single-dir source-set logic with CSV-aware logic and configurable output dir.**

In `flink-runtime/udfs/build.gradle`, replace the existing `def workspaceUdfDir = project.findProperty('workspaceUdfDir')` block and `sourceSets { ... }` block, and the `shadowJar { ... }` block's `destinationDirectory` line.

Replace this section:

```groovy
// Support compiling UDFs from workspace directory (user's project)
// Pass via: ./gradlew shadowJar -PworkspaceUdfDir=/path/to/workspace/udfs
def workspaceUdfDir = project.findProperty('workspaceUdfDir')

sourceSets {
    main {
        java {
            // Always include bundled examples (read-only, shipped with extension)
            srcDir 'src/main/java'

            // Include workspace UDFs if directory is specified and exists
            if (workspaceUdfDir) {
                def workspaceSourceDir = file("${workspaceUdfDir}/src/main/java")
                if (workspaceSourceDir.exists()) {
                    srcDir workspaceSourceDir
                    println "Including workspace UDFs from: ${workspaceSourceDir}"
                } else {
                    println "Workspace UDF directory not found: ${workspaceSourceDir}"
                    println "Only bundled examples will be compiled."
                }
            } else {
                println "No workspace UDF directory specified (-PworkspaceUdfDir)"
                println "Only bundled examples will be compiled."
            }
        }
    }
}
```

With this:

```groovy
// Support compiling UDFs from one or more source directories.
// Pass via: ./gradlew :udfs:shadowJar -PudfSourceDirs=/path/a/src/main/java,/path/b/src/main/java
//   - Each path must point to a `src/main/java`-style root.
//   - Empty/missing paths are logged and skipped.
def udfSourceDirsProp = project.findProperty('udfSourceDirs') ?: ''
def udfSourceDirs = udfSourceDirsProp
    .split(',')
    .collect { it.trim() }
    .findAll { !it.isEmpty() }

sourceSets {
    main {
        java {
            // Always include bundled examples (shipped with the extension).
            srcDir 'src/main/java'

            udfSourceDirs.each { dirPath ->
                def dir = file(dirPath)
                if (dir.exists()) {
                    srcDir dir
                    println "Including UDF source dir: ${dir}"
                } else {
                    println "UDF source dir not found, skipping: ${dir}"
                }
            }

            if (udfSourceDirs.isEmpty()) {
                println "No -PudfSourceDirs specified; building bundled examples only."
            }
        }
    }
}
```

Then locate this line in the `shadowJar { ... }` block:

```groovy
    // Output directly to lib directory
    destinationDirectory = file('../lib')
```

Replace it with:

```groovy
    // Output to a caller-specified directory (workspace/.flink-udfs by default
    // when invoked from the extension). Falls back to ../lib when unset so
    // that running gradle directly from the CLI still works.
    def udfOutputDirProp = project.findProperty('udfOutputDir')
    destinationDirectory = udfOutputDirProp ? file(udfOutputDirProp) : file('../lib')
```

- [ ] **Step 2: Verify gradle still parses by running a dry build.**

Run from `flink-runtime/`:
```bash
./gradlew :udfs:tasks --quiet
```
Expected: lists tasks without configuration errors. If gradle complains about syntax, fix the build.gradle and re-run.

- [ ] **Step 3: Verify the new properties work end-to-end with a temporary output dir.**

Run from `flink-runtime/`:
```bash
./gradlew :udfs:shadowJar -PudfSourceDirs= -PudfOutputDir=/tmp/flink-udfs-test
```
Expected: `BUILD SUCCESSFUL`, and `/tmp/flink-udfs-test/flink-udfs.jar` exists. Clean up: `rm -rf /tmp/flink-udfs-test`.

- [ ] **Step 4: Commit.**

```bash
git add flink-runtime/udfs/build.gradle
git commit -m "refactor(udfs): accept udfSourceDirs (CSV) and udfOutputDir gradle properties"
```

---

## Task 2: Add `udfSourceDirs` setting and flip `udfAutoBuild` default to false

**Files:**
- Modify: `vscode-extension/package.json:319-328`

- [ ] **Step 1: Update the contributed settings block.**

Replace the existing `udfAutoRegister` + `udfAutoBuild` settings with:

```json
        "flink-notebooks.udfAutoRegister": {
          "type": "boolean",
          "default": true,
          "description": "Automatically register UDFs (ADD JAR + CREATE TEMPORARY FUNCTION) when a new SQL Gateway session is created."
        },
        "flink-notebooks.udfAutoBuild": {
          "type": "boolean",
          "default": false,
          "description": "Automatically rebuild UDFs and recycle the session when a UDF .java file is saved. Off by default; rebuilds can take several seconds and recycle the active session."
        },
        "flink-notebooks.udfSourceDirs": {
          "type": "array",
          "items": { "type": "string" },
          "default": [],
          "description": "Additional absolute paths to UDF source directories (each must contain `src/main/java/...`). The workspace `udfs/` directory is always included. Useful for importing UDFs from external Flink projects without copying files."
        }
```

- [ ] **Step 2: Compile to confirm the manifest is still valid JSON and the extension builds.**

Run from `vscode-extension/`:
```bash
npm run compile:extension
```
Expected: TypeScript compiles cleanly (no manifest validation runs at compile time, but a JSON parse error elsewhere would block compile). Then sanity-check the JSON itself:
```bash
node -e "JSON.parse(require('fs').readFileSync('package.json','utf8'))" && echo OK
```
Expected: `OK`.

- [ ] **Step 3: Commit.**

```bash
git add vscode-extension/package.json
git commit -m "feat(udfs): add udfSourceDirs setting; default udfAutoBuild to false"
```

---

## Task 3: Add jar-path + ADD JAR helpers to UdfManager (no behavior change yet)

**Files:**
- Modify: `vscode-extension/src/services/udfManager.ts`

- [ ] **Step 1: Add a `getUdfJarPath()` method.**

In `UdfManager`, add this method right after `getFlinkRuntimePath()` (around line 184):

```typescript
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
```

- [ ] **Step 2: Add an `addJarToSession()` method.**

In `UdfManager`, add this method right after `registerUdf()` (just before `registerAllUdfs`, around line 393):

```typescript
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
```

- [ ] **Step 3: Update `registerAllUdfs()` to call `addJarToSession()` first.**

Replace the existing `registerAllUdfs` method body (around line 397-426) with:

```typescript
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
```

- [ ] **Step 4: Compile.**

Run from `vscode-extension/`:
```bash
npm run compile:extension
```
Expected: clean compile.

- [ ] **Step 5: Commit.**

```bash
git add vscode-extension/src/services/udfManager.ts
git commit -m "feat(udfs): add ADD JAR support to UdfManager.registerAllUdfs"
```

---

## Task 4: Make UdfManager scan multiple source dirs and use new gradle properties

**Files:**
- Modify: `vscode-extension/src/services/udfManager.ts`

- [ ] **Step 1: Add a helper that returns all configured source dirs.**

In `UdfManager`, add this method right after `initializePaths()` (around line 168):

```typescript
  /**
   * All UDF source directories the build should compile from. Always includes
   * the workspace default; appends user-configured `udfSourceDirs` paths.
   */
  private getAllUdfSourceDirs(): string[] {
    const dirs: string[] = [this.getUdfSourcePath()];

    const config = vscode.workspace.getConfiguration("flink-notebooks");
    const extra = config.get<string[]>("udfSourceDirs", []);

    for (const dir of extra) {
      const trimmed = dir.trim();
      if (!trimmed) continue;
      if (!path.isAbsolute(trimmed)) {
        if (this.logger) {
          this.logger.error(
            `udfSourceDirs entry must be absolute, skipping: ${trimmed}`,
          );
        }
        continue;
      }
      dirs.push(trimmed);
    }

    return dirs;
  }
```

- [ ] **Step 2: Update `scanUdfs()` to scan all configured dirs.**

Replace the body of `scanUdfs()` (the section after the initial `initializePaths` try/catch, starting around line 443) with the multi-dir version:

```typescript
  async scanUdfs(): Promise<UdfInfo[]> {
    try {
      this.initializePaths();
    } catch (error) {
      if (this.logger) {
        this.logger.log("No workspace folder - skipping UDF scan");
      }
      return [];
    }

    const sourceDirs = this.getAllUdfSourceDirs();
    const udfs: UdfInfo[] = [];

    for (const sourceDir of sourceDirs) {
      if (!fs.existsSync(sourceDir)) continue;

      const javaFiles = fs
        .readdirSync(sourceDir)
        .filter((file) => file.endsWith(".java"));

      for (const file of javaFiles) {
        const filePath = path.join(sourceDir, file);
        const className = file.replace(".java", "");
        const content = fs.readFileSync(filePath, "utf-8");

        const functionMatch = content.match(
          /CREATE\s+TEMPORARY\s+FUNCTION\s+(\w+)/i,
        );
        const functionName = functionMatch
          ? functionMatch[1]
          : className.toLowerCase();

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
    }

    if (this.logger) {
      this.logger.log(
        `Scanned ${udfs.length} UDF(s) from ${sourceDirs.length} source dir(s)`,
      );
    }

    return udfs;
  }
```

- [ ] **Step 3: Update `buildUdfs()` to pass the new gradle properties.**

Replace the gradle invocation block inside `buildUdfs()` (the section starting `const buildProcess = spawn(...)`, around line 270) with:

```typescript
      const sourceDirs = this.getAllUdfSourceDirs();
      const sourceDirsCsv = sourceDirs.join(",");
      const outputDir = this.getUdfOutputDir();

      // Ensure output dir exists before gradle writes into it.
      if (!fs.existsSync(outputDir)) {
        fs.mkdirSync(outputDir, { recursive: true });
      }

      if (this.logger) {
        this.logger.log(`UDF source dirs: ${sourceDirsCsv}`);
        this.logger.log(`UDF output dir: ${outputDir}`);
      }

      const buildProcess = spawn(
        gradlewPath,
        [
          ":udfs:shadowJar",
          `-PudfSourceDirs=${sourceDirsCsv}`,
          `-PudfOutputDir=${outputDir}`,
        ],
        {
          cwd: flinkRuntimePath,
          shell: true,
        },
      );
```

Also remove the now-unused `workspaceUdfDir` local variable from earlier in the function (the `const workspaceUdfDir = path.join(workspaceRoot, "udfs");` line if present, around line 268).

- [ ] **Step 4: Compile.**

Run from `vscode-extension/`:
```bash
npm run compile:extension
```
Expected: clean compile.

- [ ] **Step 5: Commit.**

```bash
git add vscode-extension/src/services/udfManager.ts
git commit -m "feat(udfs): scan and build from multiple udfSourceDirs"
```

---

## Task 5: Add `recycleSession()` to SessionManager

**Files:**
- Modify: `vscode-extension/src/providers/flinkNotebookController.ts:932-996`

- [ ] **Step 1: Add the `recycleSession` method.**

Inside `class SessionManager`, after `closeSession()` (around line 995, just before the final closing `}`), add:

```typescript
  /**
   * Close the current session and open a new one. Used after rebuilding UDFs:
   * a fresh session means a fresh classloader, which loads the new UDF classes.
   * Tolerates "session already gone" during close so a dead session doesn't
   * block recycling.
   */
  async recycleSession(): Promise<string> {
    if (this.currentSessionId) {
      try {
        await this.gatewayClient.closeSession(this.currentSessionId);
      } catch (error) {
        // Session may already be gone (cluster restarted, etc.) - proceed.
        console.log("recycleSession: close failed, proceeding:", error);
      }
      this.currentSessionId = null;
    }
    return this.getOrCreateSession();
  }
```

- [ ] **Step 2: Compile.**

Run from `vscode-extension/`:
```bash
npm run compile:extension
```
Expected: clean compile.

- [ ] **Step 3: Commit.**

```bash
git add vscode-extension/src/providers/flinkNotebookController.ts
git commit -m "feat(session): add SessionManager.recycleSession"
```

---

## Task 6: Recycle session on Build UDFs

**Files:**
- Modify: `vscode-extension/src/extension.ts:613-645`

- [ ] **Step 1: Replace the `buildUdfs` command body with a recycle-aware version.**

Replace the current `flink-notebooks.buildUdfs` command (around line 613-645) with:

```typescript
  // Build UDFs
  context.subscriptions.push(
    vscode.commands.registerCommand('flink-notebooks.buildUdfs', async () => {
      try {
        vscode.window.showInformationMessage('Building UDFs...');
        await udfManager.buildUdfs();

        const clusterStatus = clusterManager.getStatus();
        const isRunning = clusterStatus === 'running';

        if (!isRunning) {
          vscode.window.showInformationMessage(
            'UDFs built. Start the cluster to load them.'
          );
          return;
        }

        // Cluster is running: recycle the session so the new jar is picked up.
        try {
          await sessionManager.recycleSession();
          vscode.window.showInformationMessage(
            'UDFs built. Session recycled - new UDFs loaded.'
          );
        } catch (recycleError) {
          vscode.window.showErrorMessage(
            `UDFs built, but session recycle failed: ${recycleError instanceof Error ? recycleError.message : String(recycleError)}`
          );
        }
      } catch (error) {
        vscode.window.showErrorMessage(
          `Failed to build UDFs: ${error instanceof Error ? error.message : String(error)}`
        );
      }
    })
  );
```

- [ ] **Step 2: Compile.**

Run from `vscode-extension/`:
```bash
npm run compile:extension
```
Expected: clean compile.

- [ ] **Step 3: Commit.**

```bash
git add vscode-extension/src/extension.ts
git commit -m "feat(udfs): recycle session on Build UDFs instead of restarting cluster"
```

---

## Task 7: Add file watcher for opt-in auto-rebuild

**Files:**
- Modify: `vscode-extension/src/extension.ts` (add inside `activate` after the existing UDF setup, around line 116)

- [ ] **Step 1: Add the file watcher block.**

Locate the end of the `if (udfAutoRegister) { ... }` block (closes around line 116) and insert immediately after it (before the `// Register notebook serializer` comment around line 118):

```typescript
  // File watcher for auto-rebuild
  const udfAutoBuild = udfConfig.get<boolean>('udfAutoBuild', false);
  if (udfAutoBuild) {
    const patterns: vscode.RelativePattern[] = [];
    const workspaceFolder = vscode.workspace.workspaceFolders?.[0];
    if (workspaceFolder) {
      patterns.push(new vscode.RelativePattern(workspaceFolder, 'udfs/src/**/*.java'));
    }
    for (const extra of udfConfig.get<string[]>('udfSourceDirs', [])) {
      const trimmed = extra?.trim();
      if (trimmed) {
        patterns.push(new vscode.RelativePattern(vscode.Uri.file(trimmed), '**/*.java'));
      }
    }

    let buildInFlight = false;
    let rebuildPending = false;
    let debounceHandle: NodeJS.Timeout | undefined;

    const triggerBuild = async () => {
      if (buildInFlight) {
        rebuildPending = true;
        return;
      }
      buildInFlight = true;
      try {
        await vscode.commands.executeCommand('flink-notebooks.buildUdfs');
      } finally {
        buildInFlight = false;
        if (rebuildPending) {
          rebuildPending = false;
          triggerBuild();
        }
      }
    };

    const onChange = () => {
      if (debounceHandle) clearTimeout(debounceHandle);
      debounceHandle = setTimeout(() => {
        debounceHandle = undefined;
        triggerBuild();
      }, 1000);
    };

    for (const pattern of patterns) {
      const watcher = vscode.workspace.createFileSystemWatcher(pattern);
      watcher.onDidChange(onChange);
      watcher.onDidCreate(onChange);
      watcher.onDidDelete(onChange);
      context.subscriptions.push(watcher);
    }

    outputChannel.appendLine(
      `UDF auto-rebuild enabled (watching ${patterns.length} pattern(s))`
    );
  }
```

- [ ] **Step 2: Compile.**

Run from `vscode-extension/`:
```bash
npm run compile:extension
```
Expected: clean compile.

- [ ] **Step 3: Commit.**

```bash
git add vscode-extension/src/extension.ts
git commit -m "feat(udfs): add opt-in auto-rebuild on file save"
```

---

## Task 8: Migrate stale `flink-runtime/lib/flink-udfs.jar` on activation

**Files:**
- Modify: `vscode-extension/src/extension.ts` (add inside `activate`, after `udfManager` is constructed, before `udfManager.scanUdfs()`)

- [ ] **Step 1: Add `path` and `fs` imports.**

`extension.ts` does not currently import `path` or `fs`. Add them to the import block at the top of the file (after the `import * as vscode from 'vscode';` line):

```typescript
import * as path from 'path';
import * as fs from 'fs';
```

- [ ] **Step 2: Add the migration block.**

Locate the line `udfManager = new UdfManager(context.extensionPath);` (around line 82) and the subsequent `setLogger` call. Right after the `setLogger` call closes (around line 87, before the `// Scan for existing UDFs` comment), insert:

```typescript
  // One-time migration: older versions wrote the UDF jar into flink-runtime/lib/,
  // which the cluster auto-loaded at startup. Dynamic loading puts it in the
  // workspace instead, so the lib/ copy is dead weight that confuses inspection.
  try {
    const staleJar = path.join(context.extensionPath, 'flink-runtime', 'lib', 'flink-udfs.jar');
    if (fs.existsSync(staleJar)) {
      fs.unlinkSync(staleJar);
      outputChannel.appendLine(
        `Migrated UDF jar to workspace; removed stale ${staleJar}`
      );
    }
  } catch (migrateError) {
    outputChannel.appendLine(
      `[WARN] UDF migration check failed: ${migrateError instanceof Error ? migrateError.message : String(migrateError)}`
    );
  }
```

- [ ] **Step 3: Compile.**

Run from `vscode-extension/`:
```bash
npm run compile:extension
```
Expected: clean compile.

- [ ] **Step 4: Commit.**

```bash
git add vscode-extension/src/extension.ts
git commit -m "feat(udfs): migrate stale flink-runtime/lib/flink-udfs.jar on activation"
```

---

## Task 9: Add `.flink-udfs/` to `.gitignore`

**Files:**
- Modify: `.gitignore` (repo root)

- [ ] **Step 1: Append the ignore pattern.**

Append to `/Users/robert/code/flink-notebooks/.gitignore`:

```
# Workspace-local UDF jar output (built by the extension).
.flink-udfs/
```

If the file already contains `.flink-udfs/`, skip this step.

- [ ] **Step 2: Commit.**

```bash
git add .gitignore
git commit -m "chore: gitignore workspace .flink-udfs/ output"
```

---

## Task 10: Manual smoke-test pass

**Files:** none (manual verification)

This task is the practical validation. The extension has no automated test infrastructure (per the design spec), so we verify the seven scenarios from the spec manually.

- [ ] **Step 1: Build the extension.**

Run from `vscode-extension/`:
```bash
npm run compile
```
Expected: clean compile.

- [ ] **Step 2: Launch Extension Development Host.**

Open `vscode-extension/` in VSCode and press F5. A new window labeled "Extension Development Host" launches.

- [ ] **Step 3: Scenario A — Cold open with no jar.**

In the dev host: open a fresh folder that has NO `.flink-udfs/` and NO `udfs/`. Open or create a `.flinknb` notebook. Cluster starts. Run a non-UDF cell like `SELECT 1;`. Open `View → Output → Flink Notebooks` and confirm the log contains `"No UDF jar at <path> - run \"Flink: Build UDFs\" to enable UDFs."`. The cell should succeed.

- [ ] **Step 4: Scenario B — Build from empty.**

Still in the dev host with the same folder. Run `Flink: Create UDF` to make a scalar UDF (e.g. class `MyUpper`, fn `my_upper`). Run `Flink: Build UDFs`. Verify:
- A jar exists at `<workspace>/.flink-udfs/flink-udfs.jar`.
- Toast: "UDFs built. Session recycled - new UDFs loaded."
- A SQL cell `SELECT my_upper('abc');` returns `ABC`.

- [ ] **Step 5: Scenario C — Edit + rebuild.**

Edit `MyUpper.java` so `eval()` prepends `"X-"` to the result. Run `Flink: Build UDFs`. Re-run the same SQL cell. Expected: result is `X-ABC`. (This proves session recycle picked up the new class definition.)

- [ ] **Step 6: Scenario D — Streaming survives recycle.**

In a new cell, run a streaming insert (e.g. an `INSERT INTO ... SELECT ... FROM source`). Confirm the job appears in the Flink Jobs tree view as RUNNING. Run `Flink: Build UDFs`. Confirm in the Jobs tree that the same job is still RUNNING (not CANCELLED).

- [ ] **Step 7: Scenario E — External source dirs.**

In another folder elsewhere on disk, create `~/tmp/extra-udfs/src/main/java/ExtraFn.java` containing a simple scalar UDF (e.g. class `ExtraFn`, function name `extra_fn`). In the extension dev host's settings, set `flink-notebooks.udfSourceDirs` to `["/Users/<you>/tmp/extra-udfs/src/main/java"]`. Run `Flink: Build UDFs`. Confirm `SELECT extra_fn(...);` works in a cell.

- [ ] **Step 8: Scenario F — Auto-rebuild toggle.**

Set `flink-notebooks.udfAutoBuild` to `true`. Reload the dev host (`Developer: Reload Window`). Edit a UDF .java file and save it. Within ~2s, watch the output channel for build + recycle log lines. Confirm the cell using that UDF picks up the change without a manual command.

- [ ] **Step 9: Scenario G — Migration.**

Quit the dev host. Manually drop a sentinel file at `vscode-extension/flink-runtime/lib/flink-udfs.jar` (copy any small jar) — wait, actually use the *extension*'s flink-runtime path. In dev mode, that's the workspace's `flink-runtime/lib/flink-udfs.jar`. Place a sentinel jar there. Relaunch the dev host. Confirm the output channel contains `"Migrated UDF jar to workspace; removed stale ..."` and the sentinel jar is gone from `flink-runtime/lib/`.

- [ ] **Step 10: If all seven scenarios pass, finalize.**

Nothing to commit (this task is verification-only). If any scenario failed, file the issue against the relevant earlier task and fix before continuing.

---

## Self-review notes

- **Spec coverage:** Sections 1-5 of the spec are each addressed: architecture (overall structure), components (Tasks 1-9), data flow (preserved by Tasks 3, 5, 6, 7), error handling (Task 3 + Task 6 catch + Task 7 coalescing), testing (Task 10).
- **Setting naming divergence:** Spec said `udfAutoRebuild`; plan uses existing `udfAutoBuild` to avoid duplicate settings. Documented at the top of this plan.
- **Open question parity:** The spec's "manual re-register has limited utility" note is preserved — Task 6 leaves the existing `flink-notebooks.registerUdfs` command in place, just no longer the primary path.
