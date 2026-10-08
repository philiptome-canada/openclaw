import { expectDefined } from "@openclaw/normalization-core/expect";
import { asOptionalRecord } from "@openclaw/normalization-core/record-coerce";
import { expect, it, vi, type Mock } from "vitest";
import { createDeferred, withinTest } from "../../../../test/helpers/promise.js";
import { replaceSessionEntry } from "../../../config/sessions/session-accessor.js";
import type { createGatewayInstanceRuntime } from "../../../gateway/server-instance-runtime.js";
import { handleChatAbortRequestWithLifecycle } from "../../../gateway/server-methods/chat-abort-handler.js";
import { requireLastRespondCall } from "../../../gateway/server-methods/chat.abort-authorization.test-helpers.js";
import {
  createChatAbortContext,
  invokeChatAbortHandler,
} from "../../../gateway/server-methods/chat.abort.test-helpers.js";
import {
  captureExecRequestOwners,
  readExecRequestOwners,
  withExecRequestOwners,
  withExecRequestTurn,
} from "../../../infra/exec-request-context.js";
import {
  consumeSelectedSystemEventEntries,
  enqueueSystemEventEntry,
} from "../../../infra/system-events.js";
import { getProcessSupervisor } from "../../../process/supervisor/index.js";
import { isPidDefinitelyDead } from "../../../shared/pid-alive.js";
import { withEnvAsync } from "../../../test-utils/env.js";
import { normalizeAcceptedSessionSpawnResult } from "../../accepted-session-spawn.js";
import { captureExecRequestCancellation } from "../../bash-process-control.js";
import {
  deleteSession,
  getSession,
  waitForExecSession,
  type ProcessSession,
} from "../../bash-process-registry.js";
import { createLazyExecTool } from "../../lazy-exec-tool.js";
import { killSubagentRunAdmin } from "../registry/subagent-control-kill.js";
import { subagentRuns } from "../registry/subagent-registry-memory.js";
import { subscribeSubagentRunChanges } from "../registry/subagent-registry-publication.js";
import type { SubagentRunRecord } from "../registry/subagent-registry.types.js";
import { enqueueSwarmRun, releaseSwarmRun } from "../swarm/swarm-scheduler.js";
import {
  createBoundSpawnInvocation,
  type createSpawnBoundaryParent,
} from "./subagent-spawn.production-boundary.test-support.js";

type BoundParent = Awaited<ReturnType<typeof createSpawnBoundaryParent>>;
type GatewayRuntime = ReturnType<typeof createGatewayInstanceRuntime>;

export function registerRequestCustodySpawnCases(options: {
  createBoundParent: () => Promise<BoundParent>;
  createBoundGateway: (bound: BoundParent) => Promise<{ runtime: GatewayRuntime }>;
  closeBoundGateway: (
    bound: BoundParent,
    runtime: GatewayRuntime,
    childRunId?: string,
  ) => Promise<unknown[]>;
  throwBoundFailures: (failures: unknown[]) => void;
  parentSessionKey: string;
  parentRunId: string;
  assertNoModelExecution: () => void;
  runEmbeddedAgent: Mock<typeof import("../../embedded-agent.js").runEmbeddedAgent>;
}) {
  const {
    createBoundParent,
    createBoundGateway,
    closeBoundGateway,
    throwBoundFailures,
    parentSessionKey,
    parentRunId,
    assertNoModelExecution,
  } = options;
  it.each(["exact", "session"] as const)(
    "stops a native child through retained original request custody after its routed turn completes (%s)",
    async (mode) => {
      const bound = await createBoundParent();
      const { runtime } = await createBoundGateway(bound);
      const original = {
        runId: "original-exec-request",
        sessionKey: "agent:main:main",
        sessionId: "original-exec-session",
        agentId: "main",
        ownerConnId: "request-owner",
      };
      await replaceSessionEntry(
        { storePath: bound.storePath, sessionKey: original.sessionKey },
        { sessionId: original.sessionId, updatedAt: Date.now() },
      );
      const owners = await withExecRequestTurn({ identity: original }, async () =>
        expectDefined(captureExecRequestOwners(original), "original request owners"),
      );
      const event = expectDefined(
        enqueueSystemEventEntry(
          "Exec completed: continue original request",
          withExecRequestOwners(
            {
              sessionKey: original.sessionKey,
            },
            owners,
          ),
        ),
        "retained request occurrence",
      );
      const groupId = "routed-native-request";
      const capacity = "routed-native-capacity";
      const started = createDeferred();
      enqueueSwarmRun({
        groupId: JSON.stringify(["main", parentSessionKey, groupId]),
        runId: capacity,
        maxConcurrent: 1,
        activeRunIds: [],
        start: async () => {
          started.resolve();
        },
        onStartFailure: () => true,
      });
      await started.promise;
      let childRunId: string | undefined;
      let childSessionKey: string | undefined;
      const failures: unknown[] = [];
      try {
        const invoke = await withExecRequestTurn(
          {
            identity: {
              runId: parentRunId,
              sessionKey: parentSessionKey,
              sessionId: "parent-session",
              agentId: "main",
            },
            owners,
          },
          async () =>
            createBoundSpawnInvocation(bound, { collect: true, groupId, context: "isolated" }),
        );
        // Tool invocation may occur outside the construction callback (including Code Mode).
        const result = await invoke();
        expect(result.details, JSON.stringify(result)).toMatchObject({
          status: "accepted",
          runId: expect.any(String),
        });
        const accepted = expectDefined(
          normalizeAcceptedSessionSpawnResult(result),
          "accepted routed native child",
        );
        childRunId = accepted.runId;
        childSessionKey = accepted.childSessionKey;
        bound.admission.close();
        bound.parent.cleanup();
        const commands = captureExecRequestCancellation(original);
        expect(commands.owners).toEqual(owners);
        const context = createChatAbortContext({ getRuntimeConfig: () => bound.cfg });
        const stop = (connId: string, exact = mode === "exact") =>
          invokeChatAbortHandler({
            handler: (requestOptions) =>
              handleChatAbortRequestWithLifecycle(requestOptions, { cascadeDescendants: true }),
            context,
            request: {
              sessionKey: original.sessionKey,
              agentId: original.agentId,
              ...(exact ? { runId: original.runId } : {}),
            },
            client: { connId, connect: { scopes: ["operator.write"] } },
          });
        const foreign = requireLastRespondCall(await stop("another-owner"));
        if (mode === "exact") {
          expect(foreign[0]).toBe(false);
          expect(foreign[2]).toMatchObject({ message: "unauthorized" });
        } else {
          expect(foreign.slice(0, 2)).toEqual([true, { ok: true, aborted: false, runIds: [] }]);
        }
        expect(subagentRuns.get(childRunId)?.execution.status).toBe("queued");
        expect(owners.every((owner) => !owner.signal.aborted)).toBe(true);
        const stopped = requireLastRespondCall(await stop("request-owner"));
        expect(stopped.slice(0, 2)).toEqual([true, { ok: true, aborted: true, runIds: [] }]);
        expect(subagentRuns.get(childRunId)).toMatchObject({
          requesterTurnRunId: parentRunId,
          execution: { status: "terminal" },
        });
        assertNoModelExecution();
        expect(captureExecRequestCancellation(original).owners).toEqual([]);
        const historical = requireLastRespondCall(await stop("request-owner", true));
        expect(historical.slice(0, 2)).toEqual([true, { ok: true, aborted: false, runIds: [] }]);
      } catch (error) {
        failures.push(error);
      } finally {
        consumeSelectedSystemEventEntries(original.sessionKey, [event]);
        if (childSessionKey) {
          try {
            await killSubagentRunAdmin({ cfg: bound.cfg, sessionKey: childSessionKey });
          } catch (error) {
            failures.push(error);
          }
        }
        releaseSwarmRun(capacity);
        failures.push(...(await closeBoundGateway(bound, runtime, childRunId)));
        throwBoundFailures(failures);
      }
    },
  );

  it("drains a completed routed native child's ordinary command without changing its receipt", async ({
    signal,
  }) => {
    const bound = await createBoundParent();
    const { runtime } = await createBoundGateway(bound);
    const original = {
      runId: "completed-child-original-request",
      sessionKey: "agent:main:main",
      sessionId: "completed-child-original-session",
      agentId: "main",
      ownerConnId: "request-owner",
    };
    await replaceSessionEntry(
      { storePath: bound.storePath, sessionKey: original.sessionKey },
      { sessionId: original.sessionId, updatedAt: Date.now() },
    );
    const owners = await withExecRequestTurn({ identity: original }, async () =>
      expectDefined(captureExecRequestOwners(original), "original request owners"),
    );
    const event = expectDefined(
      enqueueSystemEventEntry(
        "Exec completed: finish original request",
        withExecRequestOwners({ sessionKey: original.sessionKey }, owners),
      ),
      "original queued occurrence",
    );
    const commandReady = createDeferred<ProcessSession>();
    void commandReady.promise.catch(() => undefined);
    const completeModel = createDeferred();
    const terminal = createDeferred<SubagentRunRecord>();
    let childRunId: string | undefined;
    let childSessionKey: string | undefined;
    let command: ProcessSession | undefined;
    const inspectTerminal = () => {
      const current = childRunId ? subagentRuns.get(childRunId) : undefined;
      if (current?.execution.status === "terminal") {
        terminal.resolve(current);
      }
    };
    const stopObserving = subscribeSubagentRunChanges("persistence", inspectTerminal);
    const loop = await import("../../embedded-agent-runner/run-loop.js");
    const native = await import("../../embedded-agent-runner/run-orchestrator.js");
    const model = vi
      .spyOn(loop, "runPreparedEmbeddedLoop")
      .mockImplementationOnce(async (_refresh, input) => {
        const run = input.runParams;
        const sessionKey = expectDefined(run.sessionKey, "child run session key");
        const requestOwners = expectDefined(
          captureExecRequestOwners(run),
          "real child request custody",
        );
        expect(requestOwners.map((owner) => owner.identity)).toEqual([
          expect.objectContaining({ runId: run.runId, sessionId: run.sessionId, sessionKey }),
        ]);
        const exec = createLazyExecTool({
          runId: run.runId,
          sessionKey,
          sessionId: run.sessionId,
          agentId: run.agentId,
          config: run.config,
          cwd: input.workspaceDir,
          scopeKey: sessionKey,
          host: "gateway",
          mode: "full",
          ask: "off",
          allowBackground: true,
          notifyOnExit: false,
          preparedStoreEnvironment: {},
        });
        const result = await withEnvAsync({ OPENCLAW_EXEC_SHELL_SNAPSHOT: "0" }, () =>
          exec.execute(
            "completed-routed-child-command",
            {
              command: `node -e "require('fs').watch('.', () => {})"`,
              yieldMs: 10,
              timeoutSeconds: 60,
            },
            run.abortSignal,
          ),
        );
        const details = asOptionalRecord(result.details);
        expect(details?.status).toBe("running");
        if (typeof details?.sessionId !== "string") {
          throw new Error("Expected the child's running command handle");
        }
        command = expectDefined(getSession(details.sessionId), "real child command");
        expect(readExecRequestOwners(command)).toEqual(requestOwners);
        commandReady.resolve(command);
        await completeModel.promise;
        return {
          payloads: [{ text: "Completed child result" }],
          meta: {
            durationMs: 1,
            finalAssistantVisibleText: "Completed child result",
            finalAssistantRawText: "Completed child result",
          },
        };
      });
    options.runEmbeddedAgent.mockImplementationOnce(async (params) => {
      try {
        return await native.runEmbeddedAgent(params);
      } catch (error) {
        commandReady.reject(error);
        throw error;
      }
    });
    const failures: unknown[] = [];
    try {
      const invoke = await withExecRequestTurn(
        {
          identity: {
            runId: parentRunId,
            sessionKey: parentSessionKey,
            sessionId: "parent-session",
            agentId: "main",
          },
          owners,
        },
        async () =>
          createBoundSpawnInvocation(bound, {
            context: "isolated",
            cleanup: "keep",
            expectsCompletionMessage: false,
          }),
      );
      const accepted = expectDefined(
        normalizeAcceptedSessionSpawnResult(await invoke()),
        "accepted native child",
      );
      childRunId = accepted.runId;
      childSessionKey = accepted.childSessionKey;
      const runningCommand = await withinTest(commandReady.promise, signal);
      completeModel.resolve();
      inspectTerminal();
      const completed = await withinTest(terminal.promise, signal);
      expect(model).toHaveBeenCalledOnce();
      const receipt = structuredClone({
        execution: completed.execution,
        completion: completed.completion,
        endedReason: completed.endedReason,
      });
      expect(completed.execution.outcome).toMatchObject({ status: "ok" });
      expect(completed.completion?.resultText).toBe("Completed child result");
      expect(runningCommand.exited).toBe(false);
      bound.admission.close();
      bound.parent.cleanup();
      expect(captureExecRequestCancellation(original).owners).toEqual(owners);
      const stopped = requireLastRespondCall(
        await invokeChatAbortHandler({
          handler: (request) =>
            handleChatAbortRequestWithLifecycle(request, { cascadeDescendants: true }),
          context: createChatAbortContext({ getRuntimeConfig: () => bound.cfg }),
          request: {
            sessionKey: original.sessionKey,
            agentId: original.agentId,
            runId: original.runId,
          },
          client: { connId: original.ownerConnId, connect: { scopes: ["operator.write"] } },
        }),
      );
      expect(stopped.slice(0, 2)).toEqual([true, { ok: true, aborted: true, runIds: [] }]);
      expect(runningCommand).toMatchObject({ exited: true, exitReason: "manual-cancel" });
      expect(isPidDefinitelyDead(expectDefined(runningCommand.pid, "child command pid"))).toBe(
        true,
      );
      const after = expectDefined(subagentRuns.get(childRunId), "completed native child");
      expect({
        execution: after.execution,
        completion: after.completion,
        endedReason: after.endedReason,
      }).toEqual(receipt);
      expect(after.killIntent).toBeUndefined();
      expect(after.killReconciliation).toBeUndefined();
    } catch (error) {
      failures.push(error);
    } finally {
      completeModel.resolve();
      stopObserving();
      consumeSelectedSystemEventEntries(original.sessionKey, [event]);
      if (command) {
        try {
          getProcessSupervisor().cancel(command.id, "manual-cancel");
          await waitForExecSession(command);
          deleteSession(command.id);
        } catch (error) {
          failures.push(error);
        }
      }
      if (childSessionKey) {
        try {
          await killSubagentRunAdmin({ cfg: bound.cfg, sessionKey: childSessionKey });
        } catch (error) {
          failures.push(error);
        }
      }
      failures.push(...(await closeBoundGateway(bound, runtime, childRunId)));
      model.mockRestore();
      throwBoundFailures(failures);
    }
  });
}
