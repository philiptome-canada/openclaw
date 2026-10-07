import { afterEach, beforeAll, beforeEach, expect, test, vi } from "vitest";
import { withExecRequestTurn } from "../infra/exec-request-context.js";
import type { RunExit } from "../process/supervisor/types.js";
import { createDeferredCore } from "../shared/deferred.js";
import { captureExecRequestCancellation } from "./bash-process-control.js";
import { waitForExecScope } from "./bash-process-registry.js";
import { createExecTool } from "./bash-tools.exec-run.js";
import { createProcessTool } from "./bash-tools.process.js";
import { applyCodeModeCatalog } from "./code-mode.js";
import {
  createCodeModeHarness,
  resetCodeModeTestState,
  resultDetails,
} from "./code-mode.test-support.js";
import { createLazyExecTool } from "./lazy-exec-tool.js";
import { createAgentRunDirectAbortError } from "./run-termination.js";

const supervisorMockState = vi.hoisted(() => ({
  cancelReasons: [] as Array<"manual-cancel" | "overall-timeout">,
  spawnInputs: [] as Array<{ timeoutMs?: number }>,
  onSpawn: undefined as (() => void) | undefined,
  finish: new Map<string, () => void>(),
  cancelled: [] as string[],
}));

vi.mock("../process/supervisor/index.js", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../process/supervisor/index.js")>();
  let counter = 0;
  return {
    ...actual,
    getProcessSupervisor: () => ({
      spawn: async (input: { runId?: string; timeoutMs?: number }) => {
        supervisorMockState.spawnInputs.push(input);
        supervisorMockState.onSpawn?.();
        const runId = input.runId ?? `mock-run-${++counter}`;
        let settled = false;
        const completion = createDeferredCore<RunExit>();
        const settle = (
          reason: "manual-cancel" | "overall-timeout" | "exit",
          timedOut: boolean,
        ) => {
          if (settled) {
            return;
          }
          settled = true;
          completion.resolve({
            reason,
            exitCode: reason === "exit" ? 0 : null,
            exitSignal: null,
            durationMs: input.timeoutMs ?? 0,
            stdout: "",
            stderr: "",
            timedOut,
            noOutputTimedOut: false,
          });
        };
        supervisorMockState.finish.set(runId, () => settle("exit", false));
        if (input.timeoutMs !== undefined) {
          setTimeout(() => settle("overall-timeout", true), Math.max(50, input.timeoutMs));
        }
        return {
          activity: {
            get resultSettled() {
              return settled;
            },
            lastOutputAtMs: Date.now(),
          },
          runId,
          startedAtMs: Date.now(),
          stdin: undefined,
          wait: () => completion.promise,
          cancel: () => {
            supervisorMockState.cancelReasons.push("manual-cancel");
            supervisorMockState.cancelled.push(runId);
            settle("manual-cancel", false);
          },
        };
      },
      cancel: vi.fn(),
      cancelScope: vi.fn(),
    }),
  };
});

vi.mock("../infra/shell-env.js", () => ({
  getShellPathFromLoginShell: vi.fn(() => null),
  resolveShellEnvFallbackTimeoutMs: vi.fn(() => 0),
}));

vi.mock("./bash-tools.exec-host-gateway.js", () => ({
  processGatewayAllowlist: vi.fn(async () => ({})),
}));

vi.mock("./bash-tools.exec-host-node.js", () => ({
  executeNodeHostCommand: vi.fn(async () => {
    throw new Error("node host not expected in background abort tests");
  }),
}));

const BACKGROUND_HOLD_CMD =
  process.platform === "win32" ? 'node -e "setTimeout(() => {}, 1000)"' : "exec sleep 1";
const POLL_INTERVAL_MS = process.platform === "win32" ? 15 : 5;
const FINISHED_WAIT_TIMEOUT_MS = process.platform === "win32" ? 8_000 : 1_000;
const BACKGROUND_TIMEOUT_SEC = process.platform === "win32" ? 0.2 : 0.02;
const YIELDED_BACKGROUND_TIMEOUT_SEC = process.platform === "win32" ? 0.4 : 0.2;
const TEST_EXEC_DEFAULTS = {
  host: "gateway" as const,
  security: "full" as const,
  ask: "off" as const,
};

let getFinishedSession: typeof import("./bash-process-registry.js").getFinishedSession;
let getSession: typeof import("./bash-process-registry.js").getSession;
let resetProcessRegistryForTests: typeof import("./bash-process-registry.test-support.js").resetProcessRegistryForTests;
type ExecToolExecuteParams = Parameters<ReturnType<typeof createExecTool>["execute"]>[1];

const createTestExecTool = (
  defaults?: Parameters<typeof createExecTool>[0],
): ReturnType<typeof createExecTool> => createExecTool({ ...TEST_EXEC_DEFAULTS, ...defaults });

beforeAll(async () => {
  ({ getFinishedSession, getSession } = await import("./bash-process-registry.js"));
  ({ resetProcessRegistryForTests } = await import("./bash-process-registry.test-support.js"));
});

beforeEach(() => {
  vi.clearAllMocks();
  supervisorMockState.cancelReasons.length = 0;
  supervisorMockState.spawnInputs.length = 0;
  supervisorMockState.onSpawn = undefined;
  supervisorMockState.finish.clear();
  supervisorMockState.cancelled.length = 0;
});

afterEach(async () => {
  resetProcessRegistryForTests();
  vi.useRealTimers();
  await resetCodeModeTestState();
});

test.each([false, true])(
  "invocation disposal preserves yielded exec until request Stop (owned=%s)",
  async (owned) => {
    vi.useFakeTimers();
    const spawned = createDeferredCore();
    const returned = createDeferredCore<string>();
    const finishTurn = createDeferredCore();
    supervisorMockState.onSpawn = () => spawned.resolve();
    const identity = {
      runId: "yielded-stop",
      sessionKey: "agent:main:yielded-stop",
      sessionId: "yielded-session",
      agentId: "main",
    };
    const request = new AbortController();
    const invocation = new AbortController();
    const execute = async () => {
      const tool = createLazyExecTool({
        ...TEST_EXEC_DEFAULTS,
        ...(owned ? identity : {}),
        allowBackground: true,
        scopeKey: identity.sessionKey,
        notifyOnExit: false,
      });
      const result = resultDetails(
        await tool.execute(
          "ordinary-command",
          {
            command: BACKGROUND_HOLD_CMD,
            yieldMs: 10,
            timeoutSeconds: 60,
          },
          invocation.signal,
        ),
      );
      if (result.status !== "running" || typeof result.sessionId !== "string") {
        throw new Error("Expected a yielded process");
      }
      returned.resolve(result.sessionId);
      await finishTurn.promise;
    };
    const running = owned
      ? withExecRequestTurn({ identity, abortSignal: request.signal }, execute)
      : execute();
    try {
      await spawned.promise;
      await vi.advanceTimersByTimeAsync(10);
      const sessionId = await returned.promise;
      invocation.abort();
      expect(supervisorMockState.cancelled).toEqual([]);
      if (owned) {
        request.abort(createAgentRunDirectAbortError());
      } else {
        supervisorMockState.finish.get(sessionId)?.();
      }
      finishTurn.resolve();
      await running;
      await waitForExecScope(identity.sessionKey);
      expect(supervisorMockState.cancelled).toEqual(owned ? [sessionId] : []);
      expect(getFinishedSession(sessionId)).toMatchObject({
        exited: true,
        exitReason: owned ? "manual-cancel" : "exit",
      });
    } finally {
      finishTurn.resolve();
      for (const finish of supervisorMockState.finish.values()) {
        finish();
      }
      await vi.advanceTimersByTimeAsync(60_000);
      await running;
      await waitForExecScope(identity.sessionKey);
    }
  },
);

test.each([false, true])(
  "request Stop preserves tool disposal, services and new requests (Code Mode=%s)",
  async (codeMode) => {
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "Date", "performance"] });
    const sessionKey = "agent:main:request-stop";
    const source = new AbortController();
    const toolLifetime = new AbortController();
    async function launch(runId: string, independent = false) {
      return withExecRequestTurn(
        {
          identity: { runId, sessionKey, sessionId: "request-stop-session", agentId: "main" },
          abortSignal: source.signal,
        },
        async () => {
          const spawned = createDeferredCore();
          supervisorMockState.onSpawn = () => spawned.resolve();
          const tool = createLazyExecTool({
            ...TEST_EXEC_DEFAULTS,
            runId,
            sessionKey,
            sessionId: "request-stop-session",
            agentId: "main",
            scopeKey: sessionKey,
            allowBackground: true,
            notifyOnExit: false,
          });
          const args = {
            command: BACKGROUND_HOLD_CMD,
            ...(independent ? { background: true } : { yieldMs: 10 }),
            timeoutSeconds: 60,
          };
          const harness = codeMode ? createCodeModeHarness() : undefined;
          if (harness) {
            applyCodeModeCatalog({
              ...harness.ctx,
              tools: [...harness.tools, tool, createProcessTool({ scopeKey: sessionKey })],
            });
          }
          const pending = harness
            ? harness.tools[0]!.execute(
                runId,
                {
                  code: `return await exec(${JSON.stringify(args)});`,
                },
                toolLifetime.signal,
              )
            : tool.execute(runId, args, toolLifetime.signal);
          await Promise.race([
            spawned.promise,
            pending.then((result) => {
              throw new Error(`Exec did not start: ${JSON.stringify(resultDetails(result))}`);
            }),
          ]);
          await vi.advanceTimersByTimeAsync(10);
          const result = await pending;
          const details = resultDetails(result);
          if (harness) {
            expect(details.status).toBe("completed");
          }
          const processDetails = harness ? resultDetails({ details: details.value }) : details;
          if (processDetails.status !== "running" || typeof processDetails.sessionId !== "string") {
            throw new Error("Expected a supervised command");
          }
          return processDetails.sessionId;
        },
      );
    }
    const ordinary = await launch("original");
    const service = await launch("service", true);
    const cancellation = captureExecRequestCancellation({
      runId: "original",
      sessionKey,
      sessionId: "request-stop-session",
      agentId: "main",
    });
    const next = await launch("new-human-request");
    // All foreground calls and the original turn have finished successfully.
    // Disposing their tool generation is not an explicit request cancellation.
    toolLifetime.abort();
    expect(supervisorMockState.cancelled).toEqual([]);
    try {
      expect(cancellation.cancel()).toBe(true);
      await cancellation.settle();
      expect(supervisorMockState.cancelled).toEqual([ordinary]);
      expect(getFinishedSession(ordinary)).toMatchObject({
        exited: true,
        exitReason: "manual-cancel",
      });
      expect(getSession(service)?.exited).toBe(false);
      expect(getSession(next)?.exited).toBe(false);
      supervisorMockState.finish.get(service)?.();
      supervisorMockState.finish.get(next)?.();
      await waitForExecScope(sessionKey);
      expect(getFinishedSession(next)).toMatchObject({ exitCode: 0, exitReason: "exit" });
    } finally {
      for (const finish of supervisorMockState.finish.values()) {
        finish();
      }
      await vi.advanceTimersByTimeAsync(60_000);
      await waitForExecScope(sessionKey);
    }
  },
);

async function waitForFinishedSession(sessionId: string) {
  let finished = getFinishedSession(sessionId);
  await expect
    .poll(
      () => {
        finished = getFinishedSession(sessionId);
        return Boolean(finished);
      },
      {
        timeout: FINISHED_WAIT_TIMEOUT_MS,
        interval: POLL_INTERVAL_MS,
      },
    )
    .toBe(true);
  return finished;
}

async function expectBackgroundSessionTimesOut(params: {
  tool: ReturnType<typeof createExecTool>;
  executeParams: ExecToolExecuteParams;
  abortAfterStart?: boolean;
  expectedTimeoutSec: number;
}) {
  const abortController = new AbortController();
  const result = await params.tool.execute(
    "toolcall",
    params.executeParams,
    abortController.signal,
  );
  expect(result.details.status).toBe("running");
  const sessionId = (result.details as { sessionId: string }).sessionId;
  expect(supervisorMockState.spawnInputs.at(-1)?.timeoutMs).toBe(
    Math.floor(params.expectedTimeoutSec * 1000),
  );

  if (params.abortAfterStart) {
    abortController.abort();
    expect(supervisorMockState.cancelReasons).toStrictEqual([]);
    expect(getFinishedSession(sessionId)).toBeUndefined();
    expect(getSession(sessionId)?.exited).toBe(false);
  }

  const finished = await waitForFinishedSession(sessionId);
  expect(finished?.terminalStatus).toBe("failed");
}

test("background exec still times out after tool signal abort", async () => {
  const tool = createTestExecTool({ allowBackground: true, backgroundMs: 0 });
  await expectBackgroundSessionTimesOut({
    tool,
    executeParams: {
      command: BACKGROUND_HOLD_CMD,
      background: true,
      timeoutSeconds: BACKGROUND_TIMEOUT_SEC,
    },
    abortAfterStart: true,
    expectedTimeoutSec: BACKGROUND_TIMEOUT_SEC,
  });
});

test("background exec with timeout zero bypasses default timeout", async () => {
  const tool = createTestExecTool({
    allowBackground: true,
    backgroundMs: 0,
    timeoutSec: BACKGROUND_TIMEOUT_SEC,
  });
  const result = await tool.execute("toolcall", {
    command: BACKGROUND_HOLD_CMD,
    background: true,
    timeoutSeconds: 0,
  });
  expect(result.details.status).toBe("running");
  const sessionId = (result.details as { sessionId: string }).sessionId;
  expect(supervisorMockState.spawnInputs.at(-1)?.timeoutMs).toBeUndefined();
  expect(getFinishedSession(sessionId)).toBeUndefined();
  expect(getSession(sessionId)?.exited).toBe(false);
});

test("yieldMs exec without explicit timeout applies default timeout", async () => {
  const tool = createTestExecTool({
    allowBackground: true,
    backgroundMs: 10,
    timeoutSec: YIELDED_BACKGROUND_TIMEOUT_SEC,
  });
  await expectBackgroundSessionTimesOut({
    tool,
    executeParams: {
      command: BACKGROUND_HOLD_CMD,
      yieldMs: 5,
    },
    expectedTimeoutSec: YIELDED_BACKGROUND_TIMEOUT_SEC,
  });
});
