import assert from "node:assert/strict";
import test from "node:test";
import {
  executePipelineAction,
  parsePipelineActionConfig,
} from "../../src/services/pipeline-action-service.js";
import { ValidationError } from "../../src/utils/errors.js";

test("transform action reshapes payload fields into a new object", () => {
  const result = executePipelineAction({
    actionType: "TRANSFORM",
    actionConfig: {
      fields: {
        eventId: "event.id",
        customerEmail: "customer.email",
        total: "total",
        missing: "customer.phone",
      },
    },
    payload: {
      event: { id: "evt-123" },
      customer: { email: "user@example.com" },
      total: 42,
    },
  });

  assert.equal(result.status, "SUCCESS");
  assert.deepEqual(result.result, {
    eventId: "evt-123",
    customerEmail: "user@example.com",
    total: 42,
    missing: null,
  });
});

test("filter action returns filtered out when all conditions do not match", () => {
  const result = executePipelineAction({
    actionType: "FILTER",
    actionConfig: {
      conditions: [
        { path: "total", operator: "gt", value: 100 },
        { path: "customer.email", operator: "exists" },
      ],
    },
    payload: {
      total: 42,
      customer: { email: "user@example.com" },
    },
  });

  assert.equal(result.status, "FILTERED_OUT");
  assert.equal(result.result, null);
});

test("filter action supports any-match evaluation", () => {
  const result = executePipelineAction({
    actionType: "FILTER",
    actionConfig: {
      match: "any",
      conditions: [
        { path: "total", operator: "gt", value: 100 },
        { path: "priority", operator: "equals", value: "high" },
      ],
    },
    payload: {
      total: 42,
      priority: "high",
    },
  });

  assert.equal(result.status, "SUCCESS");
  assert.deepEqual(result.result, {
    total: 42,
    priority: "high",
  });
});

test("enrich action merges configured fields into the payload", () => {
  const result = executePipelineAction({
    actionType: "ENRICH",
    actionConfig: {
      add: {
        priority: "high",
        tags: ["orders", "vip"],
      },
    },
    payload: {
      eventId: "evt-123",
      total: 42,
    },
  });

  assert.equal(result.status, "SUCCESS");
  assert.deepEqual(result.result, {
    eventId: "evt-123",
    total: 42,
    priority: "high",
    tags: ["orders", "vip"],
  });
});

test("action config parser rejects invalid transform paths", () => {
  assert.throws(
    () =>
      parsePipelineActionConfig("TRANSFORM", {
        fields: {
          email: "contact.email!",
        },
      }),
    ValidationError,
  );
});

test("action execution rejects non-object payloads for transform and enrich", () => {
  assert.throws(
    () =>
      executePipelineAction({
        actionType: "TRANSFORM",
        actionConfig: {
          fields: {
            value: "total",
          },
        },
        payload: 42,
      }),
    /TRANSFORM payload must be a JSON object/,
  );

  assert.throws(
    () =>
      executePipelineAction({
        actionType: "ENRICH",
        actionConfig: {
          add: {
            priority: "high",
          },
        },
        payload: 42,
      }),
    /ENRICH payload must be a JSON object/,
  );
});
