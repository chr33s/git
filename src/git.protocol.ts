import { GitError } from "./git.error.ts";

export interface GitRepoInfo {
  host: string;
  repo: string;
}

export interface GitRef {
  oid?: string;
  name: string;
  target?: string;
}

interface GitServiceAdvertisement {
  capabilities: Set<string>;
  refs: GitRef[];
}

export class GitProtocol {
  async discoverRefs(repo: GitRepoInfo) {
    const url = `https://${repo.host}/${repo.repo}.git/info/refs?service=git-upload-pack`;

    const response = await fetch(url);
    if (!response.ok) {
      throw new GitError(`Failed to discover refs: ${response.statusText}`, "protocol_error");
    }

    const text = await response.text();
    return this.#parseRefs(text);
  }

  async fetchPack(repo: GitRepoInfo, wants: string[], haves: string[]) {
    const url = `https://${repo.host}/${repo.repo}.git/git-upload-pack`;

    // Build request body
    const lines: string[] = [];

    // Want lines
    for (let i = 0; i < wants.length; i++) {
      if (i === 0) {
        lines.push(`0032want ${wants[i]}\n`);
      } else {
        lines.push(`0032want ${wants[i]}\n`);
      }
    }

    // Have lines
    for (const have of haves) {
      lines.push(`0032have ${have}\n`);
    }

    // Done
    lines.push("0009done\n");

    const body = lines.join("");

    const response = await fetch(url, {
      method: "POST",
      headers: {
        "Content-Type": "application/x-git-upload-pack-request",
        Accept: "application/x-git-upload-pack-result",
      },
      body,
    });

    if (!response.ok) {
      throw new GitError(`Failed to fetch pack: ${response.statusText}`, "protocol_error");
    }

    if (!response.body) {
      throw new GitError("No response body", "protocol_error");
    }

    return response.body;
  }

  async pushPack(
    repo: GitRepoInfo,
    refs: Array<{ ref: string; old: string; new: string }>,
    packData: Uint8Array,
  ) {
    const advertisement = await this.#discoverServiceAdvertisement(repo, "git-receive-pack");
    const url = `https://${repo.host}/${repo.repo}.git/git-receive-pack`;

    // Build request
    const lines: string[] = [];
    const capabilities = ["report-status"];

    if (advertisement.capabilities.has("ofs-delta")) {
      capabilities.push("ofs-delta");
    }

    if (advertisement.capabilities.has("atomic")) {
      capabilities.push("atomic");
    }

    for (const [index, ref] of refs.entries()) {
      const suffix = index === 0 ? `\0${capabilities.join(" ")}` : "";
      lines.push(this.#pktLine(`${ref.old} ${ref.new} ${ref.ref}${suffix}\n`));
    }

    lines.push("0000");

    const header = new TextEncoder().encode(lines.join(""));
    const body = new Uint8Array(header.length + packData.length);
    body.set(header);
    body.set(packData, header.length);

    const response = await fetch(url, {
      method: "POST",
      headers: {
        "Content-Type": "application/x-git-receive-pack-request",
        Accept: "application/x-git-receive-pack-result",
      },
      body,
    });

    if (!response.ok) {
      throw new GitError(`Failed to push: ${response.statusText}`, "protocol_error");
    }

    const responseText = await response.text();
    this.#assertPushResult(responseText);
  }

  async #discoverServiceAdvertisement(
    repo: GitRepoInfo,
    service: "git-upload-pack" | "git-receive-pack",
  ) {
    const url = `https://${repo.host}/${repo.repo}.git/info/refs?service=${service}`;

    const response = await fetch(url);
    if (!response.ok) {
      throw new GitError(`Failed to discover refs: ${response.statusText}`, "protocol_error");
    }

    const text = await response.text();
    return this.#parseAdvertisement(text);
  }

  #pktLine(text: string) {
    const length = text.length + 4;
    return length.toString(16).padStart(4, "0") + text;
  }

  #parseAdvertisement(data: string): GitServiceAdvertisement {
    const refs: GitRef[] = [];
    const capabilities = new Set<string>();
    const packets = this.#readPktLines(data);

    for (const packet of packets) {
      const text = packet.endsWith("\n") ? packet.slice(0, -1) : packet;
      if (!text || text.startsWith("# service=")) {
        continue;
      }

      const [refPart, capabilityPart] = text.split("\0", 2);
      if (capabilityPart) {
        for (const capability of capabilityPart.trim().split(/\s+/)) {
          if (capability) {
            capabilities.add(capability);
          }

          if (capability.startsWith("symref=HEAD:")) {
            refs.push({
              name: "HEAD",
              target: capability.slice("symref=HEAD:".length),
            });
          }
        }
      }

      const match = refPart?.match(/^([0-9a-f]{40})\s+(.+?)(\^\{\})?$/);
      if (match && match[1] && match[2] && match[2] !== "capabilities^{}") {
        refs.push({
          oid: match[1],
          name: match[2],
        });
      }
    }

    return { capabilities, refs };
  }

  #assertPushResult(data: string) {
    const packets = this.#readPktLines(data);
    let unpackError: string | null = null;
    const refErrors: string[] = [];

    for (const packet of packets) {
      const text = packet.endsWith("\n") ? packet.slice(0, -1) : packet;
      if (!text) continue;

      if (text.startsWith("unpack ") && text !== "unpack ok") {
        unpackError = text.slice("unpack ".length);
      }

      if (text.startsWith("ng ")) {
        refErrors.push(text.slice(3));
      }
    }

    if (unpackError) {
      throw new GitError(`Push rejected: ${unpackError}`, "ref_conflict");
    }

    if (refErrors.length > 0) {
      throw new GitError(`Push rejected: ${refErrors.join("; ")}`, "ref_conflict");
    }
  }

  #readPktLines(data: string) {
    const packets: string[] = [];
    let index = 0;

    while (index + 4 <= data.length) {
      const lengthText = data.slice(index, index + 4);
      const length = parseInt(lengthText, 16);

      if (Number.isNaN(length)) {
        break;
      }

      index += 4;

      if (length === 0 || length === 1) {
        continue;
      }

      const contentLength = length - 4;
      if (index + contentLength > data.length) {
        break;
      }

      packets.push(data.slice(index, index + contentLength));
      index += contentLength;
    }

    return packets;
  }

  #parseRefs(data: string) {
    return this.#parseAdvertisement(data).refs;
  }
}
