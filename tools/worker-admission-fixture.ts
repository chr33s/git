import assert from "node:assert/strict";
import { request } from "node:http";
import { setTimeout as delay } from "node:timers/promises";
import { createHash } from "node:crypto";

/** The verifier configures a ten-second request deadline in both runtimes. */
const expireUpload = (
  url: string,
  method: "POST" | "PUT",
  contentType: string,
  partial: string | Uint8Array,
  authorization?: string,
  expectedStatus = 504,
) =>
  new Promise<void>((resolve, reject) => {
    const upload = request(url, {
      method,
      headers:
        authorization === undefined
          ? { "content-type": contentType }
          : { authorization, "content-type": contentType },
    });
    upload.once("response", (received) => {
      received.resume();
      received.once("error", reject);
      received.once("end", () => {
        try {
          assert.equal(received.statusCode, expectedStatus);
          assert.equal(received.headers["cache-control"], "no-store");
          resolve();
        } catch (error) {
          reject(error);
        } finally {
          upload.destroy();
        }
      });
    });
    upload.once("error", reject);
    upload.setTimeout(20_000, () =>
      upload.destroy(new Error("request deadline did not settle the upload")),
    );
    upload.write(partial);
  });

export const verifyDeadline = async (repo: string, authorization?: string) => {
  const edge = authorization === undefined;
  await expireUpload(
    `${repo}/${edge ? "git-upload-pack" : "commit"}`,
    "POST",
    edge ? "application/x-git-upload-pack-request" : "application/json",
    edge ? "00" : '{"files":',
    authorization,
  );
  const recovered = await fetch(
    edge ? `${repo}/info/refs?service=git-upload-pack` : `${repo}/refs`,
    {
      headers: authorization === undefined ? {} : { authorization },
      signal: AbortSignal.timeout(10_000),
    },
  );
  await recovered.arrayBuffer();
  assert.equal(recovered.status, 200, "the service must recover after a timed-out upload");
};

/** Uninterruptible mutation routes must still stop waiting for upload bytes. */
export const verifyMutationDeadlines = async (
  repo: string,
  authorization: string,
  expectedStatus = 504,
) => {
  const refs = async () => {
    const response = await fetch(`${repo}/refs`, {
      headers: { authorization },
      signal: AbortSignal.timeout(10_000),
    });
    assert.equal(response.status, 200);
    return response.json();
  };
  const before = await refs();
  await expireUpload(
    `${repo}/git-receive-pack`,
    "POST",
    "application/x-git-receive-pack-request",
    "00",
    authorization,
    expectedStatus,
  );
  assert.deepEqual(await refs(), before, "cancelled receive-pack must preserve refs");
  await expireUpload(
    `${repo}/commit-pack`,
    "POST",
    "application/x-ndjson",
    '{"type":"commit",',
    authorization,
    expectedStatus,
  );
  assert.deepEqual(await refs(), before, "cancelled commit-pack must preserve refs");
};

/** Cross the multipart threshold, stall, then retry the complete object. */
export const verifyLfsDeadline = async (lfs: string, authorization: string) => {
  const content = Buffer.alloc(8 * 1024 * 1024 + 32, 0x78);
  const oid = createHash("sha256").update(content).digest("hex");
  const url = `${lfs}/${oid}`;
  await expireUpload(
    url,
    "PUT",
    "application/octet-stream",
    content.subarray(0, 8 * 1024 * 1024),
    authorization,
  );
  const absent = await fetch(url, {
    headers: { authorization },
    signal: AbortSignal.timeout(10_000),
  });
  await absent.arrayBuffer();
  assert.equal(absent.status, 404, "deadline must not publish the partial LFS object");
  const retry = await fetch(url, {
    method: "PUT",
    headers: { authorization, "content-type": "application/octet-stream" },
    body: content,
    signal: AbortSignal.timeout(20_000),
  });
  await retry.arrayBuffer();
  assert.equal(retry.status, 200, "complete LFS retry must succeed after timeout cleanup");
  const received = await fetch(url, {
    headers: { authorization },
    signal: AbortSignal.timeout(10_000),
  });
  assert.equal(received.status, 200);
  assert.equal(
    createHash("sha256")
      .update(new Uint8Array(await received.arrayBuffer()))
      .digest("hex"),
    oid,
  );
};

/** Exercise the deployed handler locally with incomplete real HTTP uploads. */
export const verifyAdmission = async (
  repo: string,
  authorization?: string,
  configuredCapacity?: number,
) => {
  const edge = authorization === undefined;
  const capacity = configuredCapacity ?? (edge ? 32 : 8);
  const probeUrl = edge ? `${repo}/info/refs?service=git-upload-pack` : `${repo}/refs`;
  const statusesReceived: number[] = [];
  const uploads = Array.from({ length: capacity }, () => {
    let finish: (status: number) => void = () => undefined;
    let fail: (error: Error) => void = () => undefined;
    const completed = new Promise<number>((resolve, reject) => {
      finish = resolve;
      fail = reject;
    });
    const upload = request(`${repo}/${edge ? "git-upload-pack" : "commit"}`, {
      method: "POST",
      headers:
        authorization === undefined
          ? { "content-type": "application/x-git-upload-pack-request" }
          : { authorization, "content-type": "application/json" },
    });
    upload.on("response", (response) => {
      if (response.statusCode === 503) {
        try {
          assert.equal(response.headers["retry-after"], "1");
          assert.equal(response.headers["cache-control"], "no-store");
        } catch (error) {
          fail(error instanceof Error ? error : new Error(String(error)));
        }
      }
      statusesReceived.push(response.statusCode ?? 0);
      response.resume();
      response.once("end", () => finish(response.statusCode ?? 0));
      response.once("error", fail);
    });
    upload.once("error", fail);
    upload.setTimeout(20_000, () => upload.destroy(new Error("admission upload timed out")));
    // Observe failures immediately even while the probe is awaiting admission.
    void completed.catch(() => undefined);
    upload.write(edge ? "00" : '{"files":');
    return { upload, completed };
  });
  try {
    const deadline = Date.now() + 10_000;
    for (;;) {
      const response = await fetch(probeUrl, {
        headers: authorization === undefined ? {} : { authorization },
        signal: AbortSignal.timeout(10_000),
      });
      await response.arrayBuffer();
      if (response.status === 503) {
        assert.equal(response.headers.get("retry-after"), "1");
        assert.equal(response.headers.get("cache-control"), "no-store");
        break;
      }
      assert.equal(response.status, 200);
      // The probe can occupy the last slot while an upload arrives. Its
      // rejection proves saturation just as a rejected probe does.
      if (statusesReceived.includes(503)) break;
      assert.ok(
        Date.now() < deadline,
        `incomplete uploads must exhaust admission; early response statuses: ${JSON.stringify(statusesReceived)}`,
      );
      await delay(10);
    }
    for (const { upload } of uploads) upload.end(edge ? "00" : "false}");
    // Public Git flush probes succeed; invalid private commit schemas fail.
    const statuses = await Promise.all(uploads.map(({ completed }) => completed));
    const accepted = statuses.filter((status) => status === (edge ? 200 : 400));
    assert.ok(accepted.length >= capacity - 1, "uploads must occupy all slots except the probe");
    assert.ok(statuses.every((status) => status === (edge ? 200 : 400) || status === 503));
    const recovered = await fetch(probeUrl, {
      headers: authorization === undefined ? {} : { authorization },
      signal: AbortSignal.timeout(10_000),
    });
    await recovered.arrayBuffer();
    assert.equal(recovered.status, 200, "settled uploads must return admission capacity");
  } finally {
    for (const { upload } of uploads) upload.destroy();
  }
};
