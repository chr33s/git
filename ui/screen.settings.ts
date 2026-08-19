/**
 * Settings.
 *
 * Everything the JSON API can administer, surfaced as one card per concern:
 * identity (`/whoami`, resolved once by the shell), branches (list, delete,
 * and `reset` — moving a ref is administration, not authoring), tags, remotes
 * with their sync verbs (`/fetch`, `/push`, `/pull`), webhooks, and
 * maintenance (`/fsck`, `/gc`, `/reflog`).
 *
 * The merge-policy toggles are `ui-switch` from `@chr33s/base-wc`, authored
 * the native-first way its contract asks for: a real `<input type="checkbox">`
 * inside the element. They — and the danger zone — are the two sections the
 * server has no endpoint for yet, and both say so instead of pretending: the
 * toggles are labelled as local, the danger buttons are disabled.
 *
 * Every action reports its outcome into the card that asked for it, in one
 * line, whether it landed or refused — an admin screen that swallows a policy
 * refusal teaches its reader the wrong lesson.
 */
import { html, nothing, type TemplateResult } from "lit";
import { customElement, property, state } from "lit/decorators.js";

import {
  ApiError,
  describe,
  type GitApi,
  type Ref,
  type ReflogEntry,
  type RemoteWire,
  type WebhookWire,
  type Whoami,
  type PolicyAnswer,
} from "./api.ts";
import { GitPlusElement } from "./base.ts";

const HEADS = "refs/heads/";
const TAGS = "refs/tags/";

const short = (name: string): string =>
  name.startsWith(HEADS)
    ? name.slice(HEADS.length)
    : name.startsWith(TAGS)
      ? name.slice(TAGS.length)
      : name;

/** Read one named field off a submitted form, or null when it is not there. */
const field = (form: HTMLFormElement, name: string): string | null => {
  const element = form.elements.namedItem(name);
  return element instanceof HTMLInputElement || element instanceof HTMLSelectElement
    ? element.value
    : null;
};

@customElement("gp-settings")
export class GpSettings extends GitPlusElement {
  /** Injected by the shell so every screen shares one client. */
  api: GitApi | null = null;

  /**
   * The browser's own signing key, for the identity card — resolved lazily
   * because describing it *generates* one on first visit, which is exactly
   * what this card is for: showing the public half so an operator can grant
   * it membership.
   */
  @state() private accessor browserKey: {
    readonly fingerprint: string;
    readonly publicKey: string;
    readonly note: string | null;
  } | null = null;

  /** The branch rules in force, from `GET /policy`; `null` while unanswered. */
  @state() private accessor rules: PolicyAnswer | null = null;

  /** The `/whoami` answer, resolved once by the shell. */
  @property({ attribute: false }) accessor who: Whoami | null = null;

  @state() private accessor branches: readonly Ref[] = [];
  @state() private accessor tags: readonly Ref[] = [];
  @state() private accessor remotes: readonly RemoteWire[] = [];
  @state() private accessor webhooks: readonly WebhookWire[] = [];
  @state() private accessor reflog: readonly ReflogEntry[] | null = null;
  @state() private accessor offline = false;
  /** The offline canary failed with a refusal, not an outage. */
  @state() private accessor denied = false;
  /** One outcome line per card, keyed by the card that asked. */
  @state() private accessor notes: Readonly<Record<string, string>> = {};
  @state() private accessor busy = false;

  override connectedCallback(): void {
    super.connectedCallback();
    void this.#load();
    void import("./identity.ts")
      .then(async (identity) => {
        this.browserKey = await identity.describeIdentity();
      })
      .catch(() => {});
  }

  async #load(): Promise<void> {
    const api = this.api;
    if (api === null) {
      this.offline = true;
      return;
    }
    try {
      // The canary: a branch list every readable repository answers. The
      // rest settle one by one, because the administrative registries can
      // refuse a reader (`repo.admin`) whose branches and policy are still
      // theirs to see — one refused card must not blank the other five.
      this.branches = (await api.branches()).filter((ref) => ref.name.startsWith(HEADS));
      this.offline = false;
      this.denied = false;
      const [tags, remotes, webhooks, rules] = await Promise.all([
        api.tags().catch((): readonly Ref[] => []),
        api.remotes().catch((): readonly RemoteWire[] => []),
        api.webhooks().catch((): readonly WebhookWire[] => []),
        api.policy().catch((): PolicyAnswer | null => null),
      ]);
      this.tags = tags;
      this.remotes = remotes;
      this.webhooks = webhooks;
      this.rules = rules;
    } catch (error) {
      if (!(error instanceof ApiError) && !(error instanceof TypeError)) throw error;
      // A refusal is not an outage: a private repository that turned this
      // key away is reachable and saying so, and the cards should carry
      // that answer rather than "not reachable".
      this.denied = error instanceof ApiError && (error.status === 401 || error.status === 403);
      this.offline = true;
    }
  }

  get #defaultBranch(): string | null {
    const names = this.branches.map((ref) => short(ref.name));
    return names.includes("main") ? "main" : (names[0] ?? null);
  }

  #note(card: string, text: string): void {
    this.notes = { ...this.notes, [card]: text };
  }

  /** Run one action, note its outcome on `card`, and refresh the lists. */
  async #run(card: string, action: () => Promise<string>): Promise<void> {
    if (this.busy) return;
    this.busy = true;
    try {
      this.#note(card, await action());
      await this.#load();
    } catch (error) {
      if (!(error instanceof ApiError) && !(error instanceof TypeError)) throw error;
      this.#note(card, describe(error));
    } finally {
      this.busy = false;
    }
  }

  protected override render(): TemplateResult {
    return html`
      <div class="gp-screen">
        <div class="gp-settings">
          <h1 class="gp-heading">Settings</h1>

          ${this.#general()} ${this.#identity()} ${this.#branches()} ${this.#tags()}
          ${this.#remotes()} ${this.#webhooks()} ${this.#maintenance()} ${this.#policy()}
          ${this.#danger()}
        </div>
      </div>
    `;
  }

  #general(): TemplateResult {
    return html`
      <section class="gp-setting-card">
        <h2 class="gp-setting-title">General</h2>
        <div class="gp-field-label">Repository name</div>
        <div class="gp-field-value">${this.api?.repo ?? "—"}</div>
        <div class="gp-field-label">
          Default
          branch${
            this.branches.length > 1
              ? html` <span class="gp-field-note">of ${this.branches.length}</span>`
              : ""
          }
        </div>
        <div class="gp-field-value">${this.#defaultBranch ?? "—"}</div>
      </section>
    `;
  }

  /**
   * Who the server says is asking, in full.
   *
   * The rail shows only the subject; this card shows what that identity may
   * actually do — which matters now that the UI writes commits and moves refs.
   */
  #identity(): TemplateResult {
    const who = this.who;
    const verdicts = who === null ? [] : Object.entries(who.branches);
    return html`
      <section class="gp-setting-card" data-card="identity">
        <h2 class="gp-setting-title" data-with-hint>Identity</h2>
        <p class="gp-setting-hint">What <code>/whoami</code> answers for this session.</p>
        <div class="gp-field-label">Subject</div>
        <div class="gp-field-value">${who?.subject ?? "anonymous"}</div>
        <div class="gp-field-label">Member</div>
        <div class="gp-field-value">
          ${who === null ? "—" : who.member ? "yes" : (who.why ?? "no")}
        </div>
        <div class="gp-field-label">Capabilities</div>
        <div class="gp-field-value">
          ${who === null || who.capabilities.length === 0 ? "none" : who.capabilities.join(", ")}
        </div>
        ${
          who?.expiresAt == null
            ? nothing
            : html`
                <div class="gp-field-label">Grant expires</div>
                <div class="gp-field-value">${who.expiresAt}</div>
              `
        }
        ${
          who?.trust == null
            ? nothing
            : html`
                <div class="gp-field-label">Trust freshness</div>
                <div class="gp-field-value">
                  ${who.trust.fresh ? "fresh" : (who.trust.reason ?? "stale")} — bounded to
                  ${String(who.trust.maxTrustAgeSeconds)}s
                </div>
              `
        }
        ${
          who?.budget == null
            ? nothing
            : html`
                <div class="gp-field-label">Usage budget</div>
                <div class="gp-field-value">
                  ${String(who.budget.usedTokens)} of ${String(who.budget.maxUsageTokens)} tokens
                  used in ${String(who.budget.windowSeconds)}s —
                  ${String(who.budget.remainingTokens)} left
                </div>
              `
        }
        <div class="gp-field-label">Browser signing key</div>
        ${
          this.browserKey === null
            ? html`<div class="gp-field-value">—</div>`
            : html`
                <div class="gp-field-value">${this.browserKey.fingerprint}</div>
                <div class="gp-field-value gp-field-value--row">
                  <input
                    class="gp-input"
                    readonly
                    aria-label="Browser public key"
                    .value=${this.browserKey.publicKey}
                    @focus=${(event: FocusEvent) => {
                      if (event.target instanceof HTMLInputElement) event.target.select();
                    }}
                  />
                  <button
                    class="gp-btn-quiet"
                    type="button"
                    @click=${() => {
                      if (this.browserKey !== null) {
                        void navigator.clipboard.writeText(this.browserKey.publicKey);
                      }
                    }}
                  >
                    Copy
                  </button>
                </div>
                ${
                  this.browserKey.note === null
                    ? nothing
                    : html`<p class="gp-notice" data-error>${this.browserKey.note}</p>`
                }
                <p class="gp-setting-hint">
                  Hub events this browser writes are signed with this key. Grant it membership with
                  <code>git+ hub grant</code> to have a repository with a genesis accept them.
                </p>
              `
        }
        ${
          verdicts.length === 0
            ? nothing
            : html`
                <div class="gp-field-label">Branch verdicts</div>
                ${verdicts.map(
                  ([name, verdict]) => html`
                    <div class="gp-field-value">
                      ${name}: push
                      ${verdict.push}${
                        verdict.why.length === 0 ? "" : ` — ${verdict.why.join("; ")}`
                      }
                    </div>
                  `,
                )}
              `
        }
      </section>
    `;
  }

  #branches(): TemplateResult {
    const fallback = this.#defaultBranch;
    return html`
      <section class="gp-setting-card" data-card="branches">
        <h2 class="gp-setting-title" data-with-hint>Branches</h2>
        <p class="gp-setting-hint">
          Delete removes the ref; move is <code>reset</code> — it points a branch at any commit or
          ref, and the reflog below remembers where it was.
        </p>
        ${this.#rows(
          this.branches,
          (ref) => html`
            <div class="gp-admin-row">
              <span class="gp-admin-name">${short(ref.name)}</span>
              <span class="gp-sha">${ref.oid.slice(0, 7)}</span>
              <button
                class="gp-btn-quiet"
                type="button"
                ?disabled=${this.busy || this.offline || short(ref.name) === fallback}
                title=${
                  short(ref.name) === fallback ? "The default branch stays" : "Delete this branch"
                }
                @click=${() =>
                  void this.#run(
                    "branches",
                    async () =>
                      `${short(ref.name)} ${(await this.api?.branchDelete(short(ref.name))) === true ? "deleted" : "was already gone"}`,
                  )}
              >
                Delete
              </button>
            </div>
          `,
        )}
        <form class="gp-admin-form" @submit=${this.#reset}>
          <select class="gp-input gp-admin-select" name="ref" aria-label="Branch to move">
            ${this.branches.map(
              (ref) => html`<option value=${short(ref.name)}>${short(ref.name)}</option>`,
            )}
          </select>
          <input
            class="gp-input"
            name="to"
            required
            autocomplete="off"
            spellcheck="false"
            placeholder="move to — an oid or a ref"
            aria-label="Target commit or ref"
          />
          <button class="gp-btn-quiet" type="submit" ?disabled=${this.busy || this.offline}>
            Move
          </button>
        </form>
        ${this.#outcome("branches")}
      </section>
    `;
  }

  #reset = (event: SubmitEvent): void => {
    event.preventDefault();
    const form = event.currentTarget;
    if (!(form instanceof HTMLFormElement)) return;
    const ref = field(form, "ref");
    const to = field(form, "to")?.trim() ?? "";
    if (ref === null || ref === "" || to === "") return;
    void this.#run("branches", async () => {
      const api = this.api;
      if (api === null) return "the git+ API is not running";
      const moved = await api.reset(ref, to);
      form.reset();
      return `${short(moved.ref)} moved ${moved.previous === null ? "into existence" : `from ${moved.previous.slice(0, 7)}`} to ${moved.oid.slice(0, 7)}`;
    });
  };

  #tags(): TemplateResult {
    return html`
      <section class="gp-setting-card" data-card="tags">
        <h2 class="gp-setting-title" data-with-hint>Tags</h2>
        <p class="gp-setting-hint">A message makes the tag annotated; none makes it lightweight.</p>
        ${this.#rows(
          this.tags,
          (ref) => html`
            <div class="gp-admin-row">
              <span class="gp-admin-name">${short(ref.name)}</span>
              <span class="gp-sha">${ref.oid.slice(0, 7)}</span>
              <button
                class="gp-btn-quiet"
                type="button"
                ?disabled=${this.busy || this.offline}
                @click=${() =>
                  void this.#run(
                    "tags",
                    async () =>
                      `${short(ref.name)} ${(await this.api?.tagDelete(short(ref.name))) === true ? "deleted" : "was already gone"}`,
                  )}
              >
                Delete
              </button>
            </div>
          `,
        )}
        <form class="gp-admin-form" @submit=${this.#tagCreate}>
          <input
            class="gp-input"
            name="name"
            required
            autocomplete="off"
            spellcheck="false"
            placeholder="v1.0.0"
            aria-label="Tag name"
          />
          <input
            class="gp-input"
            name="message"
            autocomplete="off"
            placeholder="message (optional)"
            aria-label="Tag message"
          />
          <button class="gp-btn-quiet" type="submit" ?disabled=${this.busy || this.offline}>
            Tag ${this.#defaultBranch ?? "the tip"}
          </button>
        </form>
        ${this.#outcome("tags")}
      </section>
    `;
  }

  #tagCreate = (event: SubmitEvent): void => {
    event.preventDefault();
    const form = event.currentTarget;
    if (!(form instanceof HTMLFormElement)) return;
    const name = field(form, "name")?.trim() ?? "";
    const message = field(form, "message")?.trim() ?? "";
    const target = this.#defaultBranch;
    if (name === "" || target === null) return;
    void this.#run("tags", async () => {
      const api = this.api;
      if (api === null) return "the git+ API is not running";
      const created = await api.tagCreate(
        message === ""
          ? { name, target: `${HEADS}${target}` }
          : { name, target: `${HEADS}${target}`, message },
      );
      form.reset();
      return `${short(created.ref)} → ${created.target.slice(0, 7)}${message === "" ? "" : " (annotated)"}`;
    });
  };

  #remotes(): TemplateResult {
    return html`
      <section class="gp-setting-card" data-card="remotes">
        <h2 class="gp-setting-title" data-with-hint>Remotes</h2>
        <p class="gp-setting-hint">
          Registered once, then fetched, pushed or pulled by name — a stored credential never comes
          back out.
        </p>
        ${this.#rows(
          this.remotes,
          (remote) => html`
            <div class="gp-admin-row" data-wide>
              <span class="gp-admin-name">${remote.name}</span>
              <span class="gp-admin-url" title=${remote.url}>${remote.url}</span>
              ${remote.has_credential ? html`<span class="gp-field-note">credential</span>` : nothing}
              ${remote.has_key ? html`<span class="gp-field-note">key</span>` : nothing}
              ${
                remote.sync === null
                  ? nothing
                  : html`<span class="gp-field-note" title="Standing sync instruction"
                      >${remote.sync.mode}</span
                    >`
              }
              <span class="gp-admin-actions">
                <button
                  class="gp-btn-quiet"
                  type="button"
                  ?disabled=${this.busy}
                  @click=${() =>
                    void this.#run("remotes", async () => {
                      const result = await this.api?.fetchRemote(remote.name);
                      return result === undefined
                        ? "the git+ API is not running"
                        : `fetched ${String(result.refs.length)} refs, ${String(result.objects)} objects into ${result.remote}`;
                    })}
                >
                  Fetch
                </button>
                <button
                  class="gp-btn-quiet"
                  type="button"
                  ?disabled=${this.busy || this.#defaultBranch === null}
                  @click=${() =>
                    void this.#run("remotes", async () => {
                      const branch = this.#defaultBranch;
                      if (branch === null) return "no branch to push";
                      const result = await this.api?.pushRemote(remote.name, branch);
                      const lines = result?.refs ?? [];
                      const failed = lines.filter((line) => !line.ok);
                      return failed.length === 0
                        ? `pushed ${branch} to ${remote.name}`
                        : failed
                            .map((line) => `${line.ref}: ${line.reason ?? "refused"}`)
                            .join("; ");
                    })}
                >
                  Push
                </button>
                <button
                  class="gp-btn-quiet"
                  type="button"
                  ?disabled=${this.busy || this.#defaultBranch === null}
                  @click=${() =>
                    void this.#run("remotes", async () => {
                      const branch = this.#defaultBranch;
                      if (branch === null) return "no branch to pull";
                      const result = await this.api?.pullRemote(remote.name, branch);
                      return result === undefined
                        ? "the git+ API is not running"
                        : result.kind === "non-fast-forward"
                          ? `${branch} diverged from ${remote.name} — merge or rebase, a pull cannot guess which`
                          : `${branch}: ${result.kind}, now ${result.to.slice(0, 7)}`;
                    })}
                >
                  Pull
                </button>
                <button
                  class="gp-btn-quiet"
                  type="button"
                  ?disabled=${this.busy}
                  @click=${() =>
                    void this.#run(
                      "remotes",
                      async () =>
                        `${remote.name} ${(await this.api?.remoteDelete(remote.name)) === true ? "removed" : "was already gone"}`,
                    )}
                >
                  Delete
                </button>
              </span>
            </div>
          `,
        )}
        <form class="gp-admin-form" @submit=${this.#remoteAdd}>
          <input
            class="gp-input gp-admin-select"
            name="name"
            required
            autocomplete="off"
            spellcheck="false"
            placeholder="origin"
            aria-label="Remote name"
          />
          <input
            class="gp-input"
            name="url"
            required
            autocomplete="off"
            spellcheck="false"
            placeholder="https://git.example.com/repo"
            aria-label="Remote URL"
          />
          <input
            class="gp-input"
            name="credential"
            type="password"
            autocomplete="off"
            placeholder="token (optional)"
            aria-label="Credential"
          />
          <button class="gp-btn-quiet" type="submit" ?disabled=${this.busy || this.offline}>
            Add
          </button>
        </form>
        ${this.#outcome("remotes")}
      </section>
    `;
  }

  #remoteAdd = (event: SubmitEvent): void => {
    event.preventDefault();
    const form = event.currentTarget;
    if (!(form instanceof HTMLFormElement)) return;
    const name = field(form, "name")?.trim() ?? "";
    const url = field(form, "url")?.trim() ?? "";
    const credential = field(form, "credential") ?? "";
    if (name === "" || url === "") return;
    void this.#run("remotes", async () => {
      const api = this.api;
      if (api === null) return "the git+ API is not running";
      const added = await api.remoteAdd(name, url, credential === "" ? undefined : credential);
      form.reset();
      return `${added.name} registered${added.has_credential ? " with a credential" : ""}`;
    });
  };

  #webhooks(): TemplateResult {
    return html`
      <section class="gp-setting-card" data-card="webhooks">
        <h2 class="gp-setting-title" data-with-hint>Webhooks</h2>
        <p class="gp-setting-hint">
          Signed deliveries on repository events. The secret goes in and never comes back out.
        </p>
        ${this.#rows(
          this.webhooks,
          (hook) => html`
            <div class="gp-admin-row" data-wide>
              <span class="gp-admin-url" title=${hook.url}>${hook.url}</span>
              <span class="gp-field-note">${hook.created_at.slice(0, 10)}</span>
              <button
                class="gp-btn-quiet"
                type="button"
                ?disabled=${this.busy}
                @click=${() =>
                  void this.#run(
                    "webhooks",
                    async () =>
                      `webhook ${(await this.api?.webhookDelete(hook.id)) === true ? "removed" : "was already gone"}`,
                  )}
              >
                Delete
              </button>
            </div>
          `,
        )}
        <form class="gp-admin-form" @submit=${this.#webhookAdd}>
          <input
            class="gp-input"
            name="url"
            required
            autocomplete="off"
            spellcheck="false"
            placeholder="https://ci.example.com/hooks/git-plus"
            aria-label="Webhook URL"
          />
          <input
            class="gp-input gp-admin-select"
            name="secret"
            type="password"
            required
            autocomplete="off"
            placeholder="secret"
            aria-label="Webhook secret"
          />
          <button class="gp-btn-quiet" type="submit" ?disabled=${this.busy || this.offline}>
            Add
          </button>
        </form>
        ${this.#outcome("webhooks")}
      </section>
    `;
  }

  #webhookAdd = (event: SubmitEvent): void => {
    event.preventDefault();
    const form = event.currentTarget;
    if (!(form instanceof HTMLFormElement)) return;
    const url = field(form, "url")?.trim() ?? "";
    const secret = field(form, "secret") ?? "";
    if (url === "" || secret === "") return;
    void this.#run("webhooks", async () => {
      const api = this.api;
      if (api === null) return "the git+ API is not running";
      const added = await api.webhookAdd(url, secret);
      form.reset();
      return `registered ${added.url}`;
    });
  };

  #maintenance(): TemplateResult {
    return html`
      <section class="gp-setting-card" data-card="maintenance">
        <h2 class="gp-setting-title" data-with-hint>Maintenance</h2>
        <p class="gp-setting-hint">
          Integrity proves the store still holds real git objects; collection removes what nothing
          reaches; the reflog remembers every move of a ref.
        </p>
        <div class="gp-admin-form">
          <button
            class="gp-btn-quiet"
            type="button"
            ?disabled=${this.busy || this.offline}
            @click=${() =>
              void this.#run("maintenance", async () => {
                const report = await this.api?.fsck();
                return report === undefined
                  ? "the git+ API is not running"
                  : report.ok
                    ? `fsck: ${String(report.checked)} objects checked, all sound`
                    : `fsck: ${String(report.problems.length)} problems, ${String(report.dangling_refs.length)} dangling refs`;
              })}
          >
            Check integrity
          </button>
          <button
            class="gp-btn-quiet"
            type="button"
            ?disabled=${this.busy || this.offline}
            @click=${() =>
              void this.#run("maintenance", async () => {
                const report = await this.api?.gc({ dry_run: true });
                return report === undefined
                  ? "the git+ API is not running"
                  : `gc (dry run): ${String(report.scanned)} scanned, ${String(report.reachable)} reachable, ${String(report.removed.length)} would go`;
              })}
          >
            Preview collection
          </button>
          <button
            class="gp-btn-quiet"
            type="button"
            ?disabled=${this.busy || this.offline}
            @click=${() =>
              void this.#run("maintenance", async () => {
                const report = await this.api?.gc();
                return report === undefined
                  ? "the git+ API is not running"
                  : `gc: ${String(report.removed.length)} removed of ${String(report.scanned)} scanned`;
              })}
          >
            Collect garbage
          </button>
          <button
            class="gp-btn-quiet"
            type="button"
            ?disabled=${this.busy || this.offline || this.#defaultBranch === null}
            @click=${() =>
              void this.#run("maintenance", async () => {
                const branch = this.#defaultBranch;
                const api = this.api;
                if (api === null || branch === null) return "the git+ API is not running";
                this.reflog = await api.reflog(branch);
                return `reflog of ${branch}: ${String(this.reflog.length)} entries`;
              })}
          >
            Show reflog
          </button>
        </div>
        ${
          this.reflog === null
            ? nothing
            : html`
                <div class="gp-admin-list">
                  ${this.reflog.map(
                    (entry) => html`
                      <div class="gp-admin-row" data-wide>
                        <span class="gp-sha"
                          >${entry.from?.slice(0, 7) ?? "·"} → ${entry.to?.slice(0, 7) ?? "·"}</span
                        >
                        <span class="gp-admin-url">${entry.message}</span>
                        <span class="gp-field-note">${entry.at.slice(0, 10)}</span>
                      </div>
                    `,
                  )}
                </div>
              `
        }
        ${this.#outcome("maintenance")}
      </section>
    `;
  }

  /**
   * The branch rules the repository actually enforces, from `GET /policy` —
   * and a form that publishes new ones through `policy.write`'s own door.
   * Local toggles are gone: what this card shows is what a push meets.
   */
  #policy(): TemplateResult {
    const rules = this.rules;
    return html`
      <section class="gp-setting-card" data-card="policy">
        <h2 class="gp-setting-title" data-with-hint>Branch policy</h2>
        <p class="gp-setting-hint">
          What the repository enforces on protected branches — read from
          <code>refs/meta/policy</code>, written back through it. Saving needs
          <code>policy.write</code>.
        </p>
        ${
          rules === null
            ? html`<div class="gp-field-value">
                ${this.#unavailable("the policy could not be read")}
              </div>`
            : html`
                <form
                  @submit=${(event: SubmitEvent) => {
                    event.preventDefault();
                    void this.#savePolicy(event);
                  }}
                >
                  <label class="gp-field-label" for="gp-policy-protected">Protected refs</label>
                  <input
                    id="gp-policy-protected"
                    class="gp-input"
                    name="protected"
                    autocomplete="off"
                    placeholder="refs/heads/main, refs/tags/*"
                    .value=${rules.rules.protected.join(", ")}
                  />
                  <label class="gp-field-label" for="gp-policy-approvals">Required approvals</label>
                  <input
                    id="gp-policy-approvals"
                    class="gp-input"
                    name="approvals"
                    type="number"
                    min="0"
                    .value=${String(rules.rules.requiredApprovals)}
                  />
                  <label class="gp-field-label" for="gp-policy-checks">Required checks</label>
                  <input
                    id="gp-policy-checks"
                    class="gp-input"
                    name="checks"
                    autocomplete="off"
                    placeholder="test, lint"
                    .value=${rules.rules.requiredChecks.join(", ")}
                  />
                  <label class="gp-switch-row">
                    <ui-switch class="gp-switch">
                      <input
                        type="checkbox"
                        name="requirePullRequest"
                        .checked=${rules.rules.requirePullRequest}
                      />
                      <span class="gp-switch-thumb"></span>
                    </ui-switch>
                    Require a pull request on protected branches
                  </label>
                  <label class="gp-switch-row">
                    <ui-switch class="gp-switch">
                      <input
                        type="checkbox"
                        name="requireResolvedThreads"
                        .checked=${rules.rules.requireResolvedThreads}
                      />
                      <span class="gp-switch-thumb"></span>
                    </ui-switch>
                    Require review threads resolved before merge
                  </label>
                  <div class="gp-dialog-actions">
                    <button
                      class="gp-btn-quiet"
                      type="submit"
                      ?disabled=${this.busy || this.offline}
                    >
                      Publish policy
                    </button>
                  </div>
                </form>
                <div class="gp-field-value" data-note="policy">
                  ${
                    rules.ref === null
                      ? "Defaults — nothing published yet."
                      : `Published at ${rules.ref.slice(0, 7)}.`
                  }
                </div>
              `
        }
        ${this.#outcome("policy")}
      </section>
    `;
  }

  async #savePolicy(event: SubmitEvent): Promise<void> {
    const api = this.api;
    const current = this.rules;
    const form = event.currentTarget;
    if (api === null || current === null || !(form instanceof HTMLFormElement)) return;
    const text = (name: string): string => {
      const field = form.elements.namedItem(name);
      return field instanceof HTMLInputElement ? field.value : "";
    };
    const on = (name: string): boolean => {
      const field = form.elements.namedItem(name);
      return field instanceof HTMLInputElement && field.checked;
    };
    const list = (value: string): string[] =>
      value
        .split(",")
        .map((entry) => entry.trim())
        .filter((entry) => entry !== "");

    await this.#run("policy", async () => {
      const written = await api.policyWrite({
        ...current.rules,
        protected: list(text("protected")),
        requiredApprovals: Math.max(0, Number.parseInt(text("approvals"), 10) || 0),
        requiredChecks: list(text("checks")),
        requirePullRequest: on("requirePullRequest"),
        requireResolvedThreads: on("requireResolvedThreads"),
      });
      this.rules = { rules: written.rules, ref: written.commit };
      return `policy published at ${written.commit.slice(0, 7)}`;
    });
  }

  #danger(): TemplateResult {
    return html`
      <section class="gp-danger">
        <h2 class="gp-danger-title">Danger zone</h2>
        <div class="gp-danger-row">
          <div>
            <div class="gp-danger-row-title">Archive repository</div>
            <div class="gp-danger-row-hint">Mark read-only and hide from search.</div>
          </div>
          <button class="gp-danger-btn" type="button" disabled title="No API endpoint yet">
            Archive
          </button>
        </div>
        <div class="gp-danger-row">
          <div>
            <div class="gp-danger-row-title">Delete repository</div>
            <div class="gp-danger-row-hint">Permanent. There is no undo.</div>
          </div>
          <button class="gp-danger-btn" type="button" disabled title="No API endpoint yet">
            Delete
          </button>
        </div>
      </section>
    `;
  }

  /**
   * Why a card cannot answer, in the right words: a refusal names the cure
   * (grant this browser's key), an outage names the fault. Conflating the
   * two sent operators of private repositories debugging a network that was
   * fine.
   */
  #unavailable(fallback: string): string {
    return this.denied
      ? "— this repository requires authentication; grant this browser's key to administer it."
      : this.offline
        ? "— the git+ API is not reachable."
        : `— ${fallback}.`;
  }

  /** A card's list, or why it is empty — offline and empty read differently. */
  #rows<T>(items: readonly T[], row: (item: T) => TemplateResult): TemplateResult {
    if (this.offline) {
      return html`<div class="gp-field-value">${this.#unavailable("unavailable")}</div>`;
    }
    if (items.length === 0) return html`<div class="gp-field-value">None yet.</div>`;
    return html`<div class="gp-admin-list">${items.map(row)}</div>`;
  }

  /** The one-line outcome of the card's last action. */
  #outcome(card: string): TemplateResult | typeof nothing {
    const note = this.notes[card];
    return note === undefined
      ? nothing
      : html`<p class="gp-admin-note" data-card-note=${card}>${note}</p>`;
  }
}

declare global {
  interface HTMLElementTagNameMap {
    "gp-settings": GpSettings;
  }
}
