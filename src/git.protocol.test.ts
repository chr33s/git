import * as assert from "node:assert/strict";
import { describe, it, mock } from "node:test";

import { GitProtocol } from "./git.protocol.ts";

function pktLine(text: string) {
  return (text.length + 4).toString(16).padStart(4, "0") + text;
}

void describe("GitProtocol", () => {
  void describe("constructor", () => {
    void it("should create protocol instance", () => {
      const protocol = new GitProtocol();
      assert.ok(protocol);
    });

    void it("should have discoverRefs method", () => {
      const protocol = new GitProtocol();
      assert.ok(typeof protocol.discoverRefs === "function");
    });

    void it("should have fetchPack method", () => {
      const protocol = new GitProtocol();
      assert.ok(typeof protocol.fetchPack === "function");
    });

    void it("should have pushPack method", () => {
      const protocol = new GitProtocol();
      assert.ok(typeof protocol.pushPack === "function");
    });
  });

  void describe("discoverRefs", () => {
    void it("should parse refs from response", async () => {
      const originalFetch = globalThis.fetch;
      try {
        globalThis.fetch = mock.fn(async () => ({
          ok: true,
          text: async () =>
            `${pktLine("# service=git-upload-pack\n")}0000${pktLine(`${"a".repeat(40)} refs/heads/main\0multi_ack thin-pack side-band\n`)}${pktLine(`${"b".repeat(40)} refs/heads/feature\n`)}0000`,
        })) as any;

        const protocol = new GitProtocol();
        const refs = await protocol.discoverRefs({ host: "github.com", repo: "user/repo" });

        assert.ok(Array.isArray(refs));
        assert.ok(refs.some((r) => r.name.includes("refs/heads/main")));
      } finally {
        globalThis.fetch = originalFetch;
      }
    });

    void it("should throw on failed response", async () => {
      const originalFetch = globalThis.fetch;
      try {
        globalThis.fetch = mock.fn(async () => ({
          ok: false,
          statusText: "Not Found",
        })) as any;

        const protocol = new GitProtocol();

        try {
          await protocol.discoverRefs({ host: "github.com", repo: "user/repo" });
          assert.fail("Should have thrown");
        } catch (error: any) {
          assert.ok(error.message.includes("Failed to discover refs"));
        }
      } finally {
        globalThis.fetch = originalFetch;
      }
    });

    void it("should parse symbolic refs", async () => {
      const originalFetch = globalThis.fetch;
      try {
        globalThis.fetch = mock.fn(async () => ({
          ok: true,
          text: async () =>
            `${pktLine("# service=git-upload-pack\n")}0000${pktLine(`${"a".repeat(40)} HEAD\0symref=HEAD:refs/heads/main multi_ack\n`)}0000`,
        })) as any;

        const protocol = new GitProtocol();
        const refs = await protocol.discoverRefs({ host: "github.com", repo: "user/repo" });

        const headRef = refs.find((r) => r.name === "HEAD");
        assert.ok(headRef);
        assert.equal(headRef?.target, "refs/heads/main");
      } finally {
        globalThis.fetch = originalFetch;
      }
    });
  });

  void describe("fetchPack", () => {
    void it("should send want/have/done request", async () => {
      const originalFetch = globalThis.fetch;
      let capturedBody: string | undefined;

      try {
        globalThis.fetch = mock.fn(async (_url: string, options: any) => {
          capturedBody = options?.body;
          return {
            ok: true,
            body: new ReadableStream({
              start(controller) {
                controller.enqueue(new TextEncoder().encode("PACK"));
                controller.close();
              },
            }),
          };
        }) as any;

        const protocol = new GitProtocol();
        const stream = await protocol.fetchPack(
          { host: "github.com", repo: "user/repo" },
          ["a".repeat(40)],
          ["b".repeat(40)],
        );

        assert.ok(stream);
        assert.ok(capturedBody?.includes("want"));
        assert.ok(capturedBody?.includes("have"));
        assert.ok(capturedBody?.includes("done"));
      } finally {
        globalThis.fetch = originalFetch;
      }
    });

    void it("should throw on failed response", async () => {
      const originalFetch = globalThis.fetch;
      try {
        globalThis.fetch = mock.fn(async () => ({
          ok: false,
          statusText: "Unauthorized",
        })) as any;

        const protocol = new GitProtocol();

        try {
          await protocol.fetchPack({ host: "github.com", repo: "user/repo" }, ["a".repeat(40)], []);
          assert.fail("Should have thrown");
        } catch (error: any) {
          assert.ok(error.message.includes("Failed to fetch pack"));
        }
      } finally {
        globalThis.fetch = originalFetch;
      }
    });

    void it("should throw when no response body", async () => {
      const originalFetch = globalThis.fetch;
      try {
        globalThis.fetch = mock.fn(async () => ({
          ok: true,
          body: null,
        })) as any;

        const protocol = new GitProtocol();

        try {
          await protocol.fetchPack({ host: "github.com", repo: "user/repo" }, ["a".repeat(40)], []);
          assert.fail("Should have thrown");
        } catch (error: any) {
          assert.ok(error.message.includes("No response body"));
        }
      } finally {
        globalThis.fetch = originalFetch;
      }
    });
  });

  void describe("pushPack", () => {
    void it("should send pack data to remote", async () => {
      const originalFetch = globalThis.fetch;
      let capturedUrl: string | undefined;
      let capturedHeaders: any;
      let callCount = 0;

      try {
        globalThis.fetch = mock.fn(async (url: string, options: any) => {
          callCount++;

          if (url.includes("info/refs?service=git-receive-pack")) {
            return {
              ok: true,
              text: async () =>
                `${pktLine("# service=git-receive-pack\n")}0000${pktLine(`${"0".repeat(40)} capabilities^{}\0report-status delete-refs ofs-delta atomic\n`)}0000`,
            };
          }

          capturedUrl = url;
          capturedHeaders = options?.headers;
          return { ok: true, text: async () => `${pktLine("unpack ok\n")}0000` };
        }) as any;

        const protocol = new GitProtocol();
        const packData = new Uint8Array([0x50, 0x41, 0x43, 0x4b]); // "PACK"

        await protocol.pushPack(
          { host: "github.com", repo: "user/repo" },
          [{ ref: "refs/heads/main", old: "0".repeat(40), new: "a".repeat(40) }],
          packData,
        );

        assert.ok(capturedUrl?.includes("git-receive-pack"));
        assert.equal(capturedHeaders?.["Content-Type"], "application/x-git-receive-pack-request");
        assert.equal(callCount, 2);
      } finally {
        globalThis.fetch = originalFetch;
      }
    });

    void it("should throw on failed push", async () => {
      const originalFetch = globalThis.fetch;
      try {
        globalThis.fetch = mock.fn(async (url: string) => {
          if (url.includes("info/refs?service=git-receive-pack")) {
            return {
              ok: true,
              text: async () =>
                `${pktLine("# service=git-receive-pack\n")}0000${pktLine(`${"0".repeat(40)} capabilities^{}\0report-status ofs-delta\n`)}0000`,
            };
          }

          return {
            ok: false,
            statusText: "Forbidden",
          };
        }) as any;

        const protocol = new GitProtocol();

        try {
          await protocol.pushPack(
            { host: "github.com", repo: "user/repo" },
            [{ ref: "refs/heads/main", old: "0".repeat(40), new: "a".repeat(40) }],
            new Uint8Array([]),
          );
          assert.fail("Should have thrown");
        } catch (error: any) {
          assert.ok(error.message.includes("Failed to push"));
        }
      } finally {
        globalThis.fetch = originalFetch;
      }
    });

    void it("should format ref updates correctly", async () => {
      const originalFetch = globalThis.fetch;
      let capturedBody: Uint8Array | undefined;

      try {
        globalThis.fetch = mock.fn(async (url: string, options: any) => {
          if (url.includes("info/refs?service=git-receive-pack")) {
            return {
              ok: true,
              text: async () =>
                `${pktLine("# service=git-receive-pack\n")}0000${pktLine(`${"0".repeat(40)} capabilities^{}\0report-status ofs-delta atomic\n`)}0000`,
            };
          }

          capturedBody = new Uint8Array(options?.body);
          return { ok: true, text: async () => `${pktLine("unpack ok\n")}0000` };
        }) as any;

        const protocol = new GitProtocol();
        const oldOid = "0".repeat(40);
        const newOid = "a".repeat(40);

        await protocol.pushPack(
          { host: "github.com", repo: "user/repo" },
          [{ ref: "refs/heads/main", old: oldOid, new: newOid }],
          new Uint8Array([]),
        );

        const bodyText = new TextDecoder().decode(capturedBody);
        assert.ok(bodyText.includes(oldOid));
        assert.ok(bodyText.includes(newOid));
        assert.ok(bodyText.includes("refs/heads/main"));
        assert.match(bodyText, /^[0-9a-f]{4}/);
        assert.ok(bodyText.includes("\0report-status ofs-delta atomic\n"));
      } finally {
        globalThis.fetch = originalFetch;
      }
    });

    void it("should throw when server rejects a ref update", async () => {
      const originalFetch = globalThis.fetch;

      try {
        globalThis.fetch = mock.fn(async (url: string) => {
          if (url.includes("info/refs?service=git-receive-pack")) {
            return {
              ok: true,
              text: async () =>
                `${pktLine("# service=git-receive-pack\n")}0000${pktLine(`${"a".repeat(40)} refs/heads/main\0report-status ofs-delta atomic\n`)}0000`,
            };
          }

          return {
            ok: true,
            text: async () =>
              `${pktLine("unpack ok\n")}${pktLine("ng refs/heads/main non-fast-forward\n")}0000`,
          };
        }) as any;

        const protocol = new GitProtocol();

        await assert.rejects(
          protocol.pushPack(
            { host: "github.com", repo: "user/repo" },
            [{ ref: "refs/heads/main", old: "a".repeat(40), new: "b".repeat(40) }],
            new Uint8Array([]),
          ),
          /Push rejected: refs\/heads\/main non-fast-forward/,
        );
      } finally {
        globalThis.fetch = originalFetch;
      }
    });
  });
});
