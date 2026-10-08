import type { ExecRequestOwner } from "../../../infra/exec-request-context.js";
import type { ResolvedSubagentController } from "./subagent-control-scope.js";
import type { SubagentRunRecord } from "./subagent-registry.types.js";
import { getSubagentRunRuntimeKey } from "./subagent-run-generation.js";

type ControllerIdentity = Pick<
  ResolvedSubagentController,
  "controllerSessionKey" | "controllerAgentId"
>;

const requestBindings = new WeakMap<
  object,
  { owners: readonly ExecRequestOwner[]; controller: ControllerIdentity }
>();

/** Registration publishes this relationship; IDs alone cannot reconstruct it later. */
export function bindSubagentExecRequestOwners(
  entry: SubagentRunRecord,
  owners: readonly ExecRequestOwner[] | undefined,
  controller: ControllerIdentity,
): void {
  const requesterTurnRunId = entry.requesterTurnRunId;
  const selected = requesterTurnRunId
    ? owners?.filter((owner) => owner.turnRunIds.has(requesterTurnRunId))
    : undefined;
  if (!selected?.length) {
    return;
  }
  requestBindings.set(getSubagentRunRuntimeKey(entry), {
    owners: selected,
    controller: { ...controller },
  });
}

/** The caller must first capture owners from current request custody. */
export function readSubagentExecRequestController(
  entry: SubagentRunRecord,
  owners: ReadonlySet<ExecRequestOwner>,
): ControllerIdentity | undefined {
  const binding = requestBindings.get(getSubagentRunRuntimeKey(entry));
  return binding?.owners.some((owner) => owners.has(owner)) ? binding.controller : undefined;
}
