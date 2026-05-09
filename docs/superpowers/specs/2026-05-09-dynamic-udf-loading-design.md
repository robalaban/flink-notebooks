# Dynamic UDF Loading — Design Spec

**Date:** 2026-05-09
**Branch:** `refactor/dynamic-loading`
**Status:** Approved, ready for implementation plan

## Goal

Eliminate the cluster-restart requirement when adding or changing UDFs. Today, building UDFs writes to `flink-runtime/lib/flink-udfs.jar`, which the cluster auto-loads at startup, so picking up new UDF code requires a full cluster restart (~30s). After this refactor, UDFs are loaded per-session via Flink's `ADD JAR` + `CREATE TEMPORARY FUNCTION`. Rebuilding triggers a session recycle (~ms), not a cluster restart. Streaming jobs already running on the cluster survive the recycle.

A secondary goal: support importing UDF source from other repositories (Flavor 1 from brainstorming) so users can develop UDFs that live outside this project's `udfs/` directory.

## Non-goals

- Loading prebuilt external UDF jars (Flavor 2 from brainstorming) — deferred to a future change.
- Hot class redefinition within a single session — Java classloaders don't support it cleanly; session recycle is the chosen solution.
- An automated test suite for UDFs — out of scope; project has no test infrastructure today, and adding it is a separate effort.

## Architecture

```
┌──────────────────────────┐
│  flink-runtime/lib/      │  ← connectors / hadoop / iceberg only
│  (loaded at startup)     │     no UDFs here anymore
└──────────────────────────┘

┌──────────────────────────┐
│  workspace/.flink-udfs/  │  ← built UDF jar lives here
│   flink-udfs.jar         │     gitignored
└────────────┬─────────────┘
             │ ADD JAR + CREATE TEMPORARY FUNCTION
             ▼
┌──────────────────────────┐
│  SQL Gateway Session     │  ← session-scoped classloader
│  (recycled on rebuild)   │
└──────────────────────────┘
```

UDFs become entirely session-scoped. The cluster's static classpath (`lib/`) is reserved for connectors. The SQL Gateway session loads UDFs via `ADD JAR`, which adds the jar to the session's classloader, followed by `CREATE TEMPORARY FUNCTION` for each UDF.

When a user rebuilds UDFs, the session is recycled (closed + reopened) so the new classloader picks up the new class definitions. Streaming jobs running on the cluster have their own JobIDs and are decoupled from the session — recycling does not cancel them.

## Components & changes

### `flink-runtime/udfs/build.gradle`

- Replace single `workspaceUdfDir` property with list-aware `udfSourceDirs` (comma-separated absolute paths). Each existing dir is added as a `srcDir` to the `main` sourceSet.
- Bundled examples (`src/main/java/examples/`) continue to be included unconditionally — they ship with the extension and provide a reference for users.
- Replace `shadowJar.destinationDirectory = file('../lib')` with a path passed via `udfOutputDir` property; the extension passes `<workspace>/.flink-udfs/`.

### `vscode-extension/src/services/udfManager.ts`

- `buildUdfs()`: pass `-PudfSourceDirs=<csv>` and `-PudfOutputDir=<ws>/.flink-udfs` to gradle. Source dirs always include the workspace default `<ws>/udfs/src/main/java`, plus any paths from `flink-notebooks.udfSourceDirs` setting.
- `scanUdfs()`: scan all configured source dirs (default + extra), not just the workspace default. UdfInfo entries get a `sourceDir` field so we can tell the user where each one came from.
- New `getUdfJarPath(): string` — returns `<workspace>/.flink-udfs/flink-udfs.jar`.
- New `addJarToSession(gateway, sessionHandle): Promise<void>` — runs `ADD JAR '<path>'` if jar exists; logs and returns silently if not.
- `registerAllUdfs(...)`: call `addJarToSession` *before* the `CREATE TEMPORARY FUNCTION` loop. If `addJarToSession` fails, skip the registration loop and surface the error.

### `vscode-extension/src/providers/flinkNotebookController.ts` (`SessionManager`)

- New `recycleSession(): Promise<string>` — closes the current session, opens a new one, runs the `onSessionCreated` callback. Returns the new session handle. Tolerates "session already gone" errors during close.

### `vscode-extension/src/extension.ts`

- `flink-notebooks.buildUdfs` command: after a successful build, call `sessionManager.recycleSession()`. Toast: `"UDFs built. Session recycled — new UDFs loaded."`. If the cluster is not running, skip the recycle and toast `"UDFs built. Start the cluster to load them."` (mirrors current behavior for the no-cluster case).
- `flink-notebooks.registerUdfs` command: keep as a manual re-sync escape hatch. Re-runs `ADD JAR` + `CREATE TEMPORARY FUNCTION` against the existing session without recycling. Useful if the user manually replaced the jar on disk.
- New file watcher on `<ws>/udfs/src/**/*.java` plus each path in `udfSourceDirs`. Gated by `flink-notebooks.udfAutoRebuild` (default `false`). On debounced (1000ms) save, invoke `flink-notebooks.buildUdfs`. Coalesce: if a build is in flight, mark "rebuild requested" and trigger exactly one more build when the current one finishes.
- Migration on activation: if `flink-runtime/lib/flink-udfs.jar` exists, delete it and log `"Migrated UDF jar to workspace; old lib/flink-udfs.jar removed."`. One-time per install.

### `vscode-extension/package.json`

New settings:

- `flink-notebooks.udfSourceDirs: string[]` (default `[]`) — extra absolute paths to UDF source directories. Each must contain `src/main/java/...` rooted appropriately for gradle.
- `flink-notebooks.udfAutoRebuild: boolean` (default `false`) — auto-rebuild + recycle on UDF source save.

Existing `flink-notebooks.udfAutoRegister` stays unchanged.

### `flink-runtime/conf/flink-conf.yaml` & `MiniClusterRunner.java`

No changes. The cluster continues to load `lib/*.jar` at startup; UDFs simply stop being part of `lib/`.

### `.gitignore`

Add `.flink-udfs/` at repo root. The extension does not need to manage user workspace `.gitignore` automatically — users importing the extension into a fresh workspace can be reminded via documentation.

## Data flow

### Notebook open → first cell run (cold path)

```
User opens .flinknb
  → ClusterManager.start()           [cluster up: lib/ connectors loaded]
  → User runs first cell
  → SessionManager.getOrCreateSession()
      → POST /v1/sessions
      → onSessionCreated fires:
          → udfManager.scanUdfs()         [populate registry from all source dirs]
          → udfManager.addJarToSession()  [ADD JAR '<ws>/.flink-udfs/flink-udfs.jar']
              ↳ if jar missing: log "no UDF jar yet, run Build UDFs"; skip register
          → udfManager.registerAllUdfs()  [CREATE TEMPORARY FUNCTION × N]
  → Cell SQL executes
```

### Build UDFs command (rebuild path)

```
User runs Flink: Build UDFs
  → udfManager.buildUdfs()
      → gradle :udfs:shadowJar
            -PudfSourceDirs=<default>,<extra1>,<extra2>
            -PudfOutputDir=<ws>/.flink-udfs
      → produces <ws>/.flink-udfs/flink-udfs.jar
  → if cluster running: sessionManager.recycleSession()
      → DELETE /v1/sessions/<old>
      → POST   /v1/sessions
      → onSessionCreated fires (same as cold path)
  → Toast: "UDFs built. Session recycled."
```

### Auto-rebuild on save (opt-in path)

```
User saves UDF .java file
  → FileSystemWatcher fires (matches udfs/src/**/*.java + extra dirs)
  → Debounce 1000ms
  → Execute flink-notebooks.buildUdfs (same as Build UDFs path)
```

### Streaming during recycle

A streaming INSERT running on the cluster has its own JobID. Session close cancels session-level *operations* (statement results, result fetching) but not cluster jobs. The job continues running and remains visible in the Jobs tree view. To fetch fresh results, the user re-runs the cell against the new session, which submits a new query.

## Error handling

| Failure | Behavior |
|---|---|
| Gradle build fails | `buildUdfs()` rejects with stderr; toast "Build failed: …". Session NOT recycled. |
| Jar missing on cold session create | `addJarToSession()` logs "No UDF jar at `<path>` — run Flink: Build UDFs"; returns silently. Session is healthy with no UDFs. SQL referencing a UDF fails with Flink's normal "Function not found". |
| `ADD JAR` fails (bad jar / gateway rejection) | Log full error to output channel; skip the registration loop; toast "Failed to load UDF jar — see Flink Notebooks output." |
| One `CREATE TEMPORARY FUNCTION` fails | Catch, log per-UDF, continue with the rest. (Existing behavior, preserved.) |
| Session recycle: close fails | Swallow ("session already gone"); proceed to open. |
| Session recycle: open fails | Surface as "Cluster appears unreachable — check status." Existing crash-handler flow takes over. |
| File watcher fires while build in flight | Coalesce — set "rebuild requested" flag, trigger one more build when current finishes. |
| `udfSourceDirs` setting changes | One-time hint "udfSourceDirs changed — run Build UDFs to pick up the new sources." |

## Testing

No automated test suite is added in this refactor. Manual test plan:

1. **Cold open with no jar.** Fresh workspace; open notebook; cluster starts; run a non-UDF cell. Expect: no UDFs loaded, no errors, "no UDF jar yet" log line.
2. **Build from empty.** Run Build UDFs. Expect: jar appears at `<ws>/.flink-udfs/flink-udfs.jar`, session recycles, a SQL cell using a bundled-example UDF (e.g., `HashFunction`) succeeds.
3. **Edit + rebuild.** Modify a UDF's `eval()` body; Build UDFs; re-run cell. Expect: new behavior reflected (proves session recycle loads the new class).
4. **Streaming survives recycle.** Start a streaming INSERT; verify job in Jobs tree; Build UDFs; verify job still RUNNING in Jobs tree after recycle.
5. **External source dirs.** Set `udfSourceDirs` to a path pointing at another repo's UDF source; Build UDFs; verify external UDFs callable from SQL.
6. **Auto-rebuild toggle.** Set `udfAutoRebuild: true`; save a .java file; verify debounced rebuild + recycle fires.
7. **Migration.** Place a stale `flink-runtime/lib/flink-udfs.jar` before activation; activate; verify it's deleted and migration log line appears.
8. **Manual re-sync command.** With session running, manually replace `<ws>/.flink-udfs/flink-udfs.jar` on disk; run `Flink: Register UDFs`; verify functions re-registered without session recycle. (Note: a brand-new ADD JAR against the same session will not pick up class changes; this command is only useful if the registration list itself drifted.)

## Open questions / known limits

- **Manual re-register (`flink-notebooks.registerUdfs`) has limited utility.** Once a class is in the session classloader, re-running `ADD JAR` doesn't redefine it. The command is preserved as an escape hatch but should be documented as "use this only when the function-name registry got out of sync; for code changes, run Build UDFs."
- **`udfSourceDirs` paths are absolute.** Relative paths would need to resolve against the workspace root, which is ambiguous in multi-root workspaces. Document the absolute-path requirement; validate at use time.
- **Bundled examples come from the extension's `flink-runtime/udfs/src/main/java/examples/`** even when running in installed mode. This is already handled by `findProjectRoot()` which falls back to `extensionPath`. The output dir change in this refactor (to workspace `.flink-udfs/`) does not affect that resolution.
