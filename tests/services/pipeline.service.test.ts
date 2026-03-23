import test from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import {
  createPipelineService,
  Pipeline,
  PipelineInput,
  PipelineStore,
} from "../../src/services/pipeline-service";
import { ConflictError, NotFoundError } from "../../src/utils/errors";
import { parsePipelineInput } from "../../src/utils/pipeline-validation";

class InMemoryPipelineStore implements PipelineStore {
  private readonly pipelines = new Map<string, Pipeline>();

  async list(): Promise<Pipeline[]> {
    return Array.from(this.pipelines.values()).sort((left, right) =>
      right.createdAt.getTime() - left.createdAt.getTime(),
    );
  }

  async findById(id: string): Promise<Pipeline | null> {
    return this.pipelines.get(id) ?? null;
  }

  async create(input: PipelineInput): Promise<Pipeline> {
    this.assertUniqueSourcePath(input.sourcePath);

    const pipeline = this.buildPipeline(input);
    this.pipelines.set(pipeline.id, pipeline);
    return pipeline;
  }

  async update(id: string, input: PipelineInput): Promise<Pipeline | null> {
    const existingPipeline = this.pipelines.get(id);
    if (!existingPipeline) {
      return null;
    }

    this.assertUniqueSourcePath(input.sourcePath, id);

    const updatedPipeline: Pipeline = {
      ...existingPipeline,
      name: input.name,
      sourcePath: input.sourcePath,
      actionType: input.actionType,
      actionConfig: input.actionConfig,
      active: input.active,
      subscribers: input.subscribers.map((subscriber) => ({
        id: randomUUID(),
        url: subscriber.url,
        active: subscriber.active,
        createdAt: new Date(),
        updatedAt: new Date(),
      })),
      updatedAt: new Date(),
    };

    this.pipelines.set(id, updatedPipeline);
    return updatedPipeline;
  }

  async delete(id: string): Promise<Pipeline | null> {
    const pipeline = this.pipelines.get(id) ?? null;
    if (pipeline) {
      this.pipelines.delete(id);
    }

    return pipeline;
  }

  private buildPipeline(input: PipelineInput): Pipeline {
    const now = new Date();

    return {
      id: randomUUID(),
      name: input.name,
      sourcePath: input.sourcePath,
      actionType: input.actionType,
      actionConfig: input.actionConfig,
      active: input.active,
      subscribers: input.subscribers.map((subscriber) => ({
        id: randomUUID(),
        url: subscriber.url,
        active: subscriber.active,
        createdAt: now,
        updatedAt: now,
      })),
      createdAt: now,
      updatedAt: now,
    };
  }

  private assertUniqueSourcePath(sourcePath: string, currentId?: string): void {
    for (const pipeline of this.pipelines.values()) {
      if (pipeline.sourcePath === sourcePath && pipeline.id !== currentId) {
        throw new ConflictError("sourcePath must be unique.");
      }
    }
  }
}

function buildInput(overrides: Partial<PipelineInput> = {}): PipelineInput {
  return {
    name: "Inbound sales lead",
    sourcePath: "/webhooks/sales-leads",
    actionType: "TRANSFORM",
    actionConfig: {
      fields: {
        leadId: "leadId",
        email: "contact.email",
        amount: "amount",
      },
    },
    active: true,
    subscribers: [
      {
        url: "https://example.com/hooks/sales",
        active: true,
      },
    ],
    ...overrides,
  };
}

test("pipeline service supports create, list, fetch, update, and delete", async () => {
  const service = createPipelineService(new InMemoryPipelineStore());

  const createdPipeline = await service.createPipeline(buildInput());
  const listedPipelines = await service.listPipelines();
  const fetchedPipeline = await service.getPipeline(createdPipeline.id);
  const updatedPipeline = await service.updatePipeline(
    createdPipeline.id,
    buildInput({
      name: "Inbound support events",
      sourcePath: "/webhooks/support-events",
      actionType: "ENRICH",
      actionConfig: {
        add: {
          priority: "high",
          slaHours: 4,
        },
      },
      subscribers: [
        {
          url: "https://example.com/hooks/support",
          active: true,
        },
        {
          url: "https://example.com/hooks/support-audit",
          active: false,
        },
      ],
    }),
  );
  const deletedPipeline = await service.deletePipeline(createdPipeline.id);

  assert.equal(listedPipelines.length, 1);
  assert.equal(fetchedPipeline.id, createdPipeline.id);
  assert.equal(updatedPipeline.name, "Inbound support events");
  assert.equal(updatedPipeline.subscribers.length, 2);
  assert.equal(deletedPipeline.id, createdPipeline.id);

  await assert.rejects(() => service.getPipeline(createdPipeline.id), NotFoundError);
});

test("pipeline service rejects duplicate source paths", async () => {
  const service = createPipelineService(new InMemoryPipelineStore());

  await service.createPipeline(buildInput());

  await assert.rejects(
    () =>
      service.createPipeline(
        buildInput({
          name: "Another pipeline",
        }),
      ),
    ConflictError,
  );
});

test("pipeline service returns not found on update and delete for missing records", async () => {
  const service = createPipelineService(new InMemoryPipelineStore());

  await assert.rejects(
    () => service.updatePipeline("missing-id", buildInput()),
    NotFoundError,
  );
  await assert.rejects(
    () => service.deletePipeline("missing-id"),
    NotFoundError,
  );
});

test("pipeline validation normalizes sourcePath and applies filter defaults", () => {
  const input = parsePipelineInput({
    name: "Alerts",
    sourcePath: "/alerts/critical/",
    actionType: "FILTER",
    actionConfig: {
      conditions: [{ path: "severity", operator: "equals", value: "critical" }],
    },
    subscribers: [{ url: "https://example.com/hooks/alerts" }],
  });

  assert.equal(input.sourcePath, "/alerts/critical");
  assert.equal(input.active, true);
  assert.deepEqual(input.actionConfig, {
    match: "all",
    conditions: [{ path: "severity", operator: "equals", value: "critical" }],
  });
  assert.equal(input.subscribers[0]?.active, true);
});

test("pipeline validation rejects duplicate subscribers", () => {
  assert.throws(
    () =>
      parsePipelineInput({
        name: "Alerts",
        sourcePath: "/alerts/critical",
        actionType: "FILTER",
        actionConfig: {
          conditions: [{ path: "severity", operator: "equals", value: "critical" }],
        },
        subscribers: [
          { url: "https://example.com/hooks/alerts" },
          { url: "https://example.com/hooks/alerts" },
        ],
      }),
    /duplicate URLs/,
  );
});

test("pipeline validation rejects invalid action types", () => {
  assert.throws(
    () =>
      parsePipelineInput({
        name: "Alerts",
        sourcePath: "/alerts/critical",
        actionType: "INVALID",
        actionConfig: {
          conditions: [{ path: "severity", operator: "equals", value: "critical" }],
        },
        subscribers: [{ url: "https://example.com/hooks/alerts" }],
      }),
    /actionType must be one of TRANSFORM, FILTER, or ENRICH/,
  );
});

test("pipeline validation rejects invalid subscriber URLs", () => {
  assert.throws(
    () =>
      parsePipelineInput({
        name: "Alerts",
        sourcePath: "/alerts/critical",
        actionType: "FILTER",
        actionConfig: {
          conditions: [{ path: "severity", operator: "equals", value: "critical" }],
        },
        subscribers: [{ url: "not-a-url" }],
      }),
    /valid absolute URLs/,
  );
});

test("pipeline validation rejects reserved API source paths", () => {
  assert.throws(
    () =>
      parsePipelineInput({
        name: "Alerts",
        sourcePath: "/pipelines/alerts",
        actionType: "FILTER",
        actionConfig: {
          conditions: [{ path: "severity", operator: "equals", value: "critical" }],
        },
        subscribers: [{ url: "https://example.com/hooks/alerts" }],
      }),
    /reserved API path/,
  );
});

test("pipeline validation rejects invalid actionConfig for TRANSFORM", () => {
  assert.throws(
    () =>
      parsePipelineInput({
        name: "Alerts",
        sourcePath: "/alerts/critical",
        actionType: "TRANSFORM",
        actionConfig: {
          fields: {},
        },
        subscribers: [{ url: "https://example.com/hooks/alerts" }],
      }),
    /must define at least one output field/,
  );
});

test("pipeline validation rejects invalid actionConfig for FILTER", () => {
  assert.throws(
    () =>
      parsePipelineInput({
        name: "Alerts",
        sourcePath: "/alerts/critical",
        actionType: "FILTER",
        actionConfig: {
          conditions: [{ path: "amount", operator: "gt", value: "100" }],
        },
        subscribers: [{ url: "https://example.com/hooks/alerts" }],
      }),
    /must be a number for numeric operators/,
  );
});

test("pipeline validation rejects invalid actionConfig for ENRICH", () => {
  assert.throws(
    () =>
      parsePipelineInput({
        name: "Alerts",
        sourcePath: "/alerts/critical",
        actionType: "ENRICH",
        actionConfig: {
          add: {},
        },
        subscribers: [{ url: "https://example.com/hooks/alerts" }],
      }),
    /must define at least one field/,
  );
});
