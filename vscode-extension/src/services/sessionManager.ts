import { SqlGatewayClient } from './sqlGatewayClient';

export interface SessionManagerOptions {
  executionModeProvider?: () => string;
}

interface SessionEntry {
  sessionId: string;
  sessionName: string;
}

/**
 * Manages SQL Gateway sessions by logical scope.
 *
 * Notebook execution, catalog introspection, and other background work each get
 * their own scope so metadata browsing cannot mutate a user's active notebook
 * context.
 */
export class SessionManager {
  private sessions: Map<string, SessionEntry> = new Map();
  private onSessionCreatedCallback?: (sessionId: string, scope: string) => Promise<void>;
  private executionModeProvider: () => string;

  constructor(
    private gatewayClient: SqlGatewayClient,
    options: SessionManagerOptions = {}
  ) {
    this.executionModeProvider = options.executionModeProvider || (() => 'auto');
  }

  /**
   * Set a callback to be invoked when a new session is created.
   */
  setOnSessionCreated(callback: (sessionId: string, scope: string) => Promise<void>): void {
    this.onSessionCreatedCallback = callback;
  }

  async getOrCreateSession(scope: string = 'default'): Promise<string> {
    const normalizedScope = this.normalizeScope(scope);
    const existing = this.sessions.get(normalizedScope);

    if (existing) {
      try {
        await this.gatewayClient.getSession(existing.sessionId);
        return existing.sessionId;
      } catch {
        this.sessions.delete(normalizedScope);
      }
    }

    const executionMode = this.executionModeProvider();
    const properties: Record<string, string> = {};
    if (executionMode !== 'auto') {
      properties['execution.runtime-mode'] = executionMode;
      console.log(`Creating Flink session with execution mode: ${executionMode}`);
    } else {
      console.log('Creating Flink session with auto execution mode (Flink decides)');
    }

    const sessionName = `notebook-${normalizedScope}`;
    const session = await this.gatewayClient.createSession(sessionName, properties);
    this.sessions.set(normalizedScope, {
      sessionId: session.sessionHandle,
      sessionName,
    });

    if (this.onSessionCreatedCallback) {
      try {
        await this.onSessionCreatedCallback(session.sessionHandle, normalizedScope);
      } catch (error) {
        console.error('Error in session created callback:', error);
      }
    }

    return session.sessionHandle;
  }

  getCurrentSessionId(scope: string = 'default'): string | null {
    return this.sessions.get(this.normalizeScope(scope))?.sessionId || null;
  }

  async closeSession(scope: string = 'default'): Promise<void> {
    const normalizedScope = this.normalizeScope(scope);
    const existing = this.sessions.get(normalizedScope);
    if (!existing) {
      return;
    }

    await this.gatewayClient.closeSession(existing.sessionId);
    this.sessions.delete(normalizedScope);
  }

  async closeAllSessions(): Promise<void> {
    const scopes = Array.from(this.sessions.keys());
    const errors: unknown[] = [];

    for (const scope of scopes) {
      try {
        await this.closeSession(scope);
      } catch (error) {
        errors.push(error);
        this.sessions.delete(scope);
      }
    }

    if (errors.length > 0) {
      throw errors[0];
    }
  }

  async recycleSession(scope: string = 'default'): Promise<string> {
    const normalizedScope = this.normalizeScope(scope);
    const existing = this.sessions.get(normalizedScope);

    if (existing) {
      try {
        await this.gatewayClient.closeSession(existing.sessionId);
      } catch (error) {
        console.log('recycleSession: close failed, proceeding:', error);
      }
      this.sessions.delete(normalizedScope);
    }

    return this.getOrCreateSession(normalizedScope);
  }

  async recycleAllSessions(): Promise<void> {
    const scopes = Array.from(this.sessions.keys());
    for (const scope of scopes) {
      await this.recycleSession(scope);
    }
  }

  private normalizeScope(scope: string): string {
    const trimmed = scope.trim();
    if (!trimmed) {
      return 'default';
    }

    return trimmed.replace(/[^a-zA-Z0-9_-]+/g, '-').replace(/^-+|-+$/g, '') || 'default';
  }
}
