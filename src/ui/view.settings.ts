/**
 * Settings.
 *
 * Everything the JSON API can administer, one card per concern: identity,
 * branches (list, delete, and `reset` — moving a ref is administration, not
 * authoring), tags, remotes with their sync verbs, webhooks, maintenance, and
 * the branch policy the repository actually enforces.
 *
 * The policy switches are `ui-switch` from `@chr33s/base-wc`, authored the
 * native-first way its contract asks for: a real checkbox inside the element.
 * The danger zone is the one section the server has no endpoint for, and it
 * says so rather than pretending — its buttons are disabled.
 *
 * Every action reports its outcome into the card that asked for it, in one
 * line, whether it landed or refused — an admin screen that swallows a policy
 * refusal teaches its reader the wrong lesson.
 */
import { AsyncData, Html } from "foldkit";

import type { Elements } from "./element.base-wc.ts";
import { AppMessage } from "./app.message.ts";
import type { Model, SettingsData, SettingsFailure } from "./app.model.ts";
import { subjectOf } from "./app.model.ts";
import { AdminAction, headRef, short } from "./settings.ts";

type H = Html.HtmlBuilder<AppMessage>;

/**
 * Why a card cannot answer, in the right words.
 *
 * A refusal names the cure, an outage names the fault. Conflating the two sent
 * operators of private repositories debugging a network that was fine.
 */
const unavailable = (failure: SettingsFailure, fallback: string): string =>
  failure === "Denied"
    ? "— this repository requires authentication; grant this browser's key to administer it."
    : failure === "Offline"
      ? "— the git+ API is not reachable."
      : `— ${fallback}.`;

/** A card's list, or why it is empty — offline and empty read differently. */
const rows = <A>(
  h: H,
  model: Model,
  items: readonly A[],
  row: (item: A) => Html.Html,
): Html.Html => {
  const failure = AsyncData.getError(model.settingsScreen.data);
  if (failure._tag === "Some") {
    return h.div([h.Class("gp-field-value")], [unavailable(failure.value, "unavailable")]);
  }
  if (items.length === 0) return h.div([h.Class("gp-field-value")], ["None yet."]);
  return h.div([h.Class("gp-admin-list")], items.map(row));
};

/** The one-line outcome of the card's last action. */
const outcome = (h: H, model: Model, card: string): Html.Html => {
  const note = model.settingsScreen.notes[card];
  return note === undefined
    ? h.empty
    : h.p([h.Class("gp-admin-note"), h.DataAttribute("card-note", card)], [note]);
};

const text = (
  h: H,
  field: keyof Model["settingsScreen"]["forms"],
  value: string,
  attrs: readonly Html.Attribute<AppMessage>[],
): Html.Html =>
  h.input([
    h.Class("gp-input"),
    h.Value(value),
    h.OnInput((next) => AppMessage.ChangedSettingsField({ field, value: next })),
    ...attrs,
  ]);

const general = (h: H, model: Model, data: SettingsData | null): Html.Html =>
  h.section(
    [h.Class("gp-setting-card")],
    [
      h.h2([h.Class("gp-setting-title")], ["General"]),
      h.div([h.Class("gp-field-label")], ["Repository name"]),
      h.div([h.Class("gp-field-value")], [model.repo]),
      h.div(
        [h.Class("gp-field-label")],
        [
          "Default branch",
          (data?.branches.length ?? 0) > 1
            ? h.span([h.Class("gp-field-note")], [` of ${String(data?.branches.length ?? 0)}`])
            : h.empty,
        ],
      ),
      h.div([h.Class("gp-field-value")], [data?.defaultBranch ?? "—"]),
    ],
  );

/**
 * Who the server says is asking, in full.
 *
 * The rail shows only the subject; this card shows what that identity may
 * actually do — which matters now that the UI writes commits and moves refs.
 */
const identity = (h: H, model: Model): Html.Html => {
  const key = model.settingsScreen.browserKey;
  const viewer = AsyncData.getOrElse(model.viewer, () => null);
  const verdicts = viewer === null ? [] : Object.entries(viewer.branches);
  return h.section(
    [h.Class("gp-setting-card"), h.DataAttribute("card", "identity")],
    [
      h.h2([h.Class("gp-setting-title"), h.DataAttribute("with-hint", "")], ["Identity"]),
      h.p(
        [h.Class("gp-setting-hint")],
        ["What ", h.code([], ["/whoami"]), " answers for this session."],
      ),
      h.div([h.Class("gp-field-label")], ["Subject"]),
      h.div([h.Class("gp-field-value")], [subjectOf(model) ?? "anonymous"]),
      h.div([h.Class("gp-field-label")], ["Member"]),
      // Not a bare "no": the server says *why* it is no — a repository with
      // no genesis has no membership to grant — and that sentence is what
      // tells an operator whether to grant a key or to create a genesis.
      h.div(
        [h.Class("gp-field-value")],
        [viewer === null ? "—" : viewer.member ? "yes" : (viewer.why ?? "no")],
      ),
      h.div([h.Class("gp-field-label")], ["Capabilities"]),
      h.div(
        [h.Class("gp-field-value")],
        [
          viewer === null || viewer.capabilities.length === 0
            ? "none"
            : viewer.capabilities.join(", "),
        ],
      ),
      ...(viewer?.expiresAt == null
        ? []
        : [
            h.div([h.Class("gp-field-label")], ["Grant expires"]),
            h.div([h.Class("gp-field-value")], [viewer.expiresAt]),
          ]),
      ...(viewer?.trust == null
        ? []
        : [
            h.div([h.Class("gp-field-label")], ["Trust freshness"]),
            h.div(
              [h.Class("gp-field-value")],
              [
                `${viewer.trust.fresh ? "fresh" : (viewer.trust.reason ?? "stale")} — bounded to ${String(viewer.trust.maxTrustAgeSeconds)}s`,
              ],
            ),
          ]),
      ...(viewer?.budget == null
        ? []
        : [
            h.div([h.Class("gp-field-label")], ["Usage budget"]),
            h.div(
              [h.Class("gp-field-value")],
              [
                `${String(viewer.budget.usedTokens)} of ${String(viewer.budget.maxUsageTokens)} tokens used in ${String(viewer.budget.windowSeconds)}s — ${String(viewer.budget.remainingTokens)} left`,
              ],
            ),
          ]),
      h.div([h.Class("gp-field-label")], ["Browser signing key"]),
      ...(key === null
        ? [h.div([h.Class("gp-field-value")], ["—"])]
        : [
            h.div([h.Class("gp-field-value")], [key.fingerprint]),
            h.div(
              [h.Class("gp-field-value gp-field-value--row")],
              [
                h.input([
                  h.Class("gp-input"),
                  h.Readonly(true),
                  h.AriaLabel("Browser public key"),
                  h.Value(key.publicKey),
                ]),
                h.button(
                  [
                    h.Class("gp-btn-quiet"),
                    h.Type("button"),
                    h.OnClick(AppMessage.ClickedCopyBrowserKey({ text: key.publicKey })),
                  ],
                  ["Copy"],
                ),
              ],
            ),
            key.note === null
              ? h.empty
              : h.p([h.Class("gp-notice"), h.DataAttribute("error", "")], [key.note]),
            h.p(
              [h.Class("gp-setting-hint")],
              [
                "Hub events this browser writes are signed with this key. Grant it membership with ",
                h.code([], ["git+ hub grant"]),
                " to have a repository with a genesis accept them.",
              ],
            ),
          ]),
      ...(verdicts.length === 0
        ? []
        : [
            h.div([h.Class("gp-field-label")], ["Branch verdicts"]),
            ...verdicts.map(([name, verdict]) =>
              h.div(
                [h.Class("gp-field-value")],
                [
                  `${name}: push ${verdict.push}${verdict.why.length === 0 ? "" : ` — ${verdict.why.join("; ")}`}`,
                ],
              ),
            ),
          ]),
    ],
  );
};

const branches = (h: H, model: Model, data: SettingsData | null): Html.Html => {
  const busy = model.settingsScreen.busy;
  const offline = data === null;
  const fallback = data?.defaultBranch ?? null;
  return h.section(
    [h.Class("gp-setting-card"), h.DataAttribute("card", "branches")],
    [
      h.h2([h.Class("gp-setting-title"), h.DataAttribute("with-hint", "")], ["Branches"]),
      h.p(
        [h.Class("gp-setting-hint")],
        [
          "Delete removes the ref; move is ",
          h.code([], ["reset"]),
          " — it points a branch at any commit or ref, and the reflog below remembers where it was.",
        ],
      ),
      rows(h, model, data?.branches ?? [], (ref) =>
        h.div(
          [h.Class("gp-admin-row")],
          [
            h.span([h.Class("gp-admin-name")], [short(ref.name)]),
            h.span([h.Class("gp-sha")], [ref.oid.slice(0, 7)]),
            h.button(
              [
                h.Class("gp-btn-quiet"),
                h.Type("button"),
                h.Disabled(busy || offline || short(ref.name) === fallback),
                h.Title(
                  short(ref.name) === fallback ? "The default branch stays" : "Delete this branch",
                ),
                h.OnClick(
                  AppMessage.SubmittedAdmin({
                    action: AdminAction.DeleteBranch({ name: short(ref.name) }),
                  }),
                ),
              ],
              ["Delete"],
            ),
          ],
        ),
      ),
      h.form(
        [
          h.Class("gp-admin-form"),
          h.OnSubmit(
            AppMessage.SubmittedAdmin({
              action: AdminAction.ResetBranch({
                ref: model.settingsScreen.forms.resetRef,
                to: model.settingsScreen.forms.resetTo,
              }),
            }),
          ),
        ],
        [
          h.select(
            [
              h.Class("gp-input gp-admin-select"),
              h.Name("ref"),
              h.AriaLabel("Branch to move"),
              h.Value(model.settingsScreen.forms.resetRef),
              h.OnChange((value) => AppMessage.ChangedSettingsField({ field: "resetRef", value })),
            ],
            (data?.branches ?? []).map((ref) =>
              h.option([h.Value(short(ref.name))], [short(ref.name)]),
            ),
          ),
          text(h, "resetTo", model.settingsScreen.forms.resetTo, [
            h.Name("to"),
            h.Required(true),
            h.Autocomplete("off"),
            h.Spellcheck(false),
            h.Placeholder("move to — an oid or a ref"),
            h.AriaLabel("Target commit or ref"),
          ]),
          h.button(
            [h.Class("gp-btn-quiet"), h.Type("submit"), h.Disabled(busy || offline)],
            ["Move"],
          ),
        ],
      ),
      outcome(h, model, "branches"),
    ],
  );
};

const tags = (h: H, model: Model, data: SettingsData | null): Html.Html => {
  const busy = model.settingsScreen.busy;
  const offline = data === null;
  const target = data?.defaultBranch ?? null;
  return h.section(
    [h.Class("gp-setting-card"), h.DataAttribute("card", "tags")],
    [
      h.h2([h.Class("gp-setting-title"), h.DataAttribute("with-hint", "")], ["Tags"]),
      h.p(
        [h.Class("gp-setting-hint")],
        ["A message makes the tag annotated; none makes it lightweight."],
      ),
      rows(h, model, data?.tags ?? [], (ref) =>
        h.div(
          [h.Class("gp-admin-row")],
          [
            h.span([h.Class("gp-admin-name")], [short(ref.name)]),
            h.span([h.Class("gp-sha")], [ref.oid.slice(0, 7)]),
            h.button(
              [
                h.Class("gp-btn-quiet"),
                h.Type("button"),
                h.Disabled(busy || offline),
                h.OnClick(
                  AppMessage.SubmittedAdmin({
                    action: AdminAction.DeleteTag({ name: short(ref.name) }),
                  }),
                ),
              ],
              ["Delete"],
            ),
          ],
        ),
      ),
      h.form(
        [
          h.Class("gp-admin-form"),
          h.OnSubmit(
            AppMessage.SubmittedAdmin({
              action: AdminAction.CreateTag({
                name: model.settingsScreen.forms.tagName,
                target: headRef(target ?? ""),
                message: model.settingsScreen.forms.tagMessage,
              }),
            }),
          ),
        ],
        [
          text(h, "tagName", model.settingsScreen.forms.tagName, [
            h.Name("name"),
            h.Required(true),
            h.Autocomplete("off"),
            h.Spellcheck(false),
            h.Placeholder("v1.0.0"),
            h.AriaLabel("Tag name"),
          ]),
          text(h, "tagMessage", model.settingsScreen.forms.tagMessage, [
            h.Name("message"),
            h.Autocomplete("off"),
            h.Placeholder("message (optional)"),
            h.AriaLabel("Tag message"),
          ]),
          h.button(
            [
              h.Class("gp-btn-quiet"),
              h.Type("submit"),
              h.Disabled(busy || offline || target === null),
            ],
            [`Tag ${target ?? "the tip"}`],
          ),
        ],
      ),
      outcome(h, model, "tags"),
    ],
  );
};

const remotes = (h: H, model: Model, data: SettingsData | null): Html.Html => {
  const busy = model.settingsScreen.busy;
  const offline = data === null;
  const branch = data?.defaultBranch ?? null;
  return h.section(
    [h.Class("gp-setting-card"), h.DataAttribute("card", "remotes")],
    [
      h.h2([h.Class("gp-setting-title"), h.DataAttribute("with-hint", "")], ["Remotes"]),
      h.p(
        [h.Class("gp-setting-hint")],
        [
          "Registered once, then fetched, pushed or pulled by name — a stored credential never comes back out.",
        ],
      ),
      rows(h, model, data?.remotes ?? [], (remote) =>
        h.div(
          [h.Class("gp-admin-row"), h.DataAttribute("wide", "")],
          [
            h.span([h.Class("gp-admin-name")], [remote.name]),
            h.span([h.Class("gp-admin-url"), h.Title(remote.url)], [remote.url]),
            remote.has_credential ? h.span([h.Class("gp-field-note")], ["credential"]) : h.empty,
            remote.has_key ? h.span([h.Class("gp-field-note")], ["key"]) : h.empty,
            remote.sync === null
              ? h.empty
              : h.span(
                  [h.Class("gp-field-note"), h.Title("Standing sync instruction")],
                  [remote.sync.mode],
                ),
            h.span(
              [h.Class("gp-admin-actions")],
              [
                h.button(
                  [
                    h.Class("gp-btn-quiet"),
                    h.Type("button"),
                    h.Disabled(busy),
                    h.OnClick(
                      AppMessage.SubmittedAdmin({
                        action: AdminAction.FetchRemote({ name: remote.name }),
                      }),
                    ),
                  ],
                  ["Fetch"],
                ),
                h.button(
                  [
                    h.Class("gp-btn-quiet"),
                    h.Type("button"),
                    h.Disabled(busy || branch === null),
                    h.OnClick(
                      AppMessage.SubmittedAdmin({
                        action: AdminAction.PushRemote({
                          name: remote.name,
                          branch: branch ?? "",
                        }),
                      }),
                    ),
                  ],
                  ["Push"],
                ),
                h.button(
                  [
                    h.Class("gp-btn-quiet"),
                    h.Type("button"),
                    h.Disabled(busy || branch === null),
                    h.OnClick(
                      AppMessage.SubmittedAdmin({
                        action: AdminAction.PullRemote({
                          name: remote.name,
                          branch: branch ?? "",
                        }),
                      }),
                    ),
                  ],
                  ["Pull"],
                ),
                h.button(
                  [
                    h.Class("gp-btn-quiet"),
                    h.Type("button"),
                    h.Disabled(busy),
                    h.OnClick(
                      AppMessage.SubmittedAdmin({
                        action: AdminAction.DeleteRemote({ name: remote.name }),
                      }),
                    ),
                  ],
                  ["Delete"],
                ),
              ],
            ),
          ],
        ),
      ),
      h.form(
        [
          h.Class("gp-admin-form"),
          h.OnSubmit(
            AppMessage.SubmittedAdmin({
              action: AdminAction.AddRemote({
                name: model.settingsScreen.forms.remoteName,
                url: model.settingsScreen.forms.remoteUrl,
                credential: model.settingsScreen.forms.remoteCredential,
              }),
            }),
          ),
        ],
        [
          text(h, "remoteName", model.settingsScreen.forms.remoteName, [
            h.Class("gp-input gp-admin-select"),
            h.Name("name"),
            h.Required(true),
            h.Autocomplete("off"),
            h.Spellcheck(false),
            h.Placeholder("origin"),
            h.AriaLabel("Remote name"),
          ]),
          text(h, "remoteUrl", model.settingsScreen.forms.remoteUrl, [
            h.Name("url"),
            h.Required(true),
            h.Autocomplete("off"),
            h.Spellcheck(false),
            h.Placeholder("https://git.example.com/repo"),
            h.AriaLabel("Remote URL"),
          ]),
          text(h, "remoteCredential", model.settingsScreen.forms.remoteCredential, [
            h.Name("credential"),
            h.Type("password"),
            h.Autocomplete("off"),
            h.Placeholder("token (optional)"),
            h.AriaLabel("Credential"),
          ]),
          h.button(
            [h.Class("gp-btn-quiet"), h.Type("submit"), h.Disabled(busy || offline)],
            ["Add"],
          ),
        ],
      ),
      outcome(h, model, "remotes"),
    ],
  );
};

const webhooks = (h: H, model: Model, data: SettingsData | null): Html.Html => {
  const busy = model.settingsScreen.busy;
  const offline = data === null;
  return h.section(
    [h.Class("gp-setting-card"), h.DataAttribute("card", "webhooks")],
    [
      h.h2([h.Class("gp-setting-title"), h.DataAttribute("with-hint", "")], ["Webhooks"]),
      h.p(
        [h.Class("gp-setting-hint")],
        ["Signed deliveries on repository events. The secret goes in and never comes back out."],
      ),
      rows(h, model, data?.webhooks ?? [], (hook) =>
        h.div(
          [h.Class("gp-admin-row"), h.DataAttribute("wide", "")],
          [
            h.span([h.Class("gp-admin-url"), h.Title(hook.url)], [hook.url]),
            h.span([h.Class("gp-field-note")], [hook.created_at.slice(0, 10)]),
            h.button(
              [
                h.Class("gp-btn-quiet"),
                h.Type("button"),
                h.Disabled(busy),
                h.OnClick(
                  AppMessage.SubmittedAdmin({
                    action: AdminAction.DeleteWebhook({ id: hook.id }),
                  }),
                ),
              ],
              ["Delete"],
            ),
          ],
        ),
      ),
      h.form(
        [
          h.Class("gp-admin-form"),
          h.OnSubmit(
            AppMessage.SubmittedAdmin({
              action: AdminAction.AddWebhook({
                url: model.settingsScreen.forms.webhookUrl,
                secret: model.settingsScreen.forms.webhookSecret,
              }),
            }),
          ),
        ],
        [
          text(h, "webhookUrl", model.settingsScreen.forms.webhookUrl, [
            h.Name("url"),
            h.Required(true),
            h.Autocomplete("off"),
            h.Spellcheck(false),
            h.Placeholder("https://ci.example.com/hooks/git-plus"),
            h.AriaLabel("Webhook URL"),
          ]),
          text(h, "webhookSecret", model.settingsScreen.forms.webhookSecret, [
            h.Class("gp-input gp-admin-select"),
            h.Name("secret"),
            h.Type("password"),
            h.Required(true),
            h.Autocomplete("off"),
            h.Placeholder("secret"),
            h.AriaLabel("Webhook secret"),
          ]),
          h.button(
            [h.Class("gp-btn-quiet"), h.Type("submit"), h.Disabled(busy || offline)],
            ["Add"],
          ),
        ],
      ),
      outcome(h, model, "webhooks"),
    ],
  );
};

const maintenance = (h: H, model: Model, data: SettingsData | null): Html.Html => {
  const busy = model.settingsScreen.busy;
  const offline = data === null;
  const branch = data?.defaultBranch ?? null;
  const reflog = model.settingsScreen.reflog;
  const action = (label: string, next: AdminAction, disabled = false): Html.Html =>
    h.button(
      [
        h.Class("gp-btn-quiet"),
        h.Type("button"),
        h.Disabled(busy || offline || disabled),
        h.OnClick(AppMessage.SubmittedAdmin({ action: next })),
      ],
      [label],
    );
  return h.section(
    [h.Class("gp-setting-card"), h.DataAttribute("card", "maintenance")],
    [
      h.h2([h.Class("gp-setting-title"), h.DataAttribute("with-hint", "")], ["Maintenance"]),
      h.p(
        [h.Class("gp-setting-hint")],
        [
          "Integrity proves the store still holds real git objects; collection removes what nothing reaches; the reflog remembers every move of a ref.",
        ],
      ),
      h.div(
        [h.Class("gp-admin-form")],
        [
          action("Check integrity", AdminAction.Fsck()),
          action("Preview collection", AdminAction.PreviewGc()),
          action("Collect garbage", AdminAction.Gc()),
          action("Show reflog", AdminAction.ShowReflog({ branch: branch ?? "" }), branch === null),
        ],
      ),
      reflog === null
        ? h.empty
        : h.div(
            [h.Class("gp-admin-list")],
            reflog.map((entry) =>
              h.div(
                [h.Class("gp-admin-row"), h.DataAttribute("wide", "")],
                [
                  h.span(
                    [h.Class("gp-sha")],
                    [`${entry.from?.slice(0, 7) ?? "·"} → ${entry.to?.slice(0, 7) ?? "·"}`],
                  ),
                  h.span([h.Class("gp-admin-url")], [entry.message]),
                  h.span([h.Class("gp-field-note")], [entry.at.slice(0, 10)]),
                ],
              ),
            ),
          ),
      outcome(h, model, "maintenance"),
    ],
  );
};

/**
 * The branch rules the repository actually enforces, and a form that publishes
 * new ones through `policy.write`'s own door. Local toggles are gone: what
 * this card shows is what a push meets.
 */
const policy = (
  h: H,
  ui: Elements<AppMessage>,
  model: Model,
  data: SettingsData | null,
): Html.Html => {
  const rules = data?.policy ?? null;
  const forms = model.settingsScreen.forms;
  const failure = AsyncData.getError(model.settingsScreen.data);
  const switchRow = (
    field: "policyRequirePullRequest" | "policyRequireResolvedThreads",
    checked: boolean,
    label: string,
  ): Html.Html =>
    h.label(
      [h.Class("gp-field-label gp-switch-row")],
      [
        ui.uiSwitch(
          [h.Class("gp-switch")],
          [
            // A controlled checkbox: `Checked` draws the Model's answer and
            // the click asks to flip it. Foldkit's `OnChange` reports
            // `event.target.value`, which for a checkbox is the constant
            // `"on"` — the useful fact is which way it was pointing, and the
            // Model already holds that.
            h.input([
              h.Type("checkbox"),
              h.Name(field),
              h.Checked(checked),
              h.OnClick(AppMessage.ToggledSettingsField({ field, value: !checked })),
            ]),
            h.span([h.Class("gp-switch-thumb")], []),
          ],
        ),
        label,
      ],
    );
  return h.section(
    [h.Class("gp-setting-card"), h.DataAttribute("card", "policy")],
    [
      h.h2([h.Class("gp-setting-title"), h.DataAttribute("with-hint", "")], ["Branch policy"]),
      h.p(
        [h.Class("gp-setting-hint")],
        [
          "What the repository enforces on protected branches — read from ",
          h.code([], ["refs/meta/policy"]),
          ", written back through it. Saving needs ",
          h.code([], ["policy.write"]),
          ".",
        ],
      ),
      rules === null
        ? h.div(
            [h.Class("gp-field-value")],
            [
              unavailable(
                failure._tag === "Some" ? failure.value : "Offline",
                "the policy could not be read",
              ),
            ],
          )
        : h.div(
            [],
            [
              h.form(
                [
                  h.OnSubmit(
                    AppMessage.SubmittedAdmin({
                      action: AdminAction.WritePolicy({
                        protectedRefs: forms.policyProtected,
                        approvals: forms.policyApprovals,
                        checks: forms.policyChecks,
                        requirePullRequest: forms.policyRequirePullRequest,
                        requireResolvedThreads: forms.policyRequireResolvedThreads,
                      }),
                    }),
                  ),
                ],
                [
                  h.label(
                    [h.Class("gp-field-label"), h.For("gp-policy-protected")],
                    ["Protected refs"],
                  ),
                  text(h, "policyProtected", forms.policyProtected, [
                    h.Id("gp-policy-protected"),
                    h.Name("protected"),
                    h.Autocomplete("off"),
                    h.Placeholder("refs/heads/main, refs/tags/*"),
                  ]),
                  h.label(
                    [h.Class("gp-field-label"), h.For("gp-policy-approvals")],
                    ["Required approvals"],
                  ),
                  text(h, "policyApprovals", forms.policyApprovals, [
                    h.Id("gp-policy-approvals"),
                    h.Name("approvals"),
                    h.Type("number"),
                    h.Min("0"),
                  ]),
                  h.label(
                    [h.Class("gp-field-label"), h.For("gp-policy-checks")],
                    ["Required checks"],
                  ),
                  text(h, "policyChecks", forms.policyChecks, [
                    h.Id("gp-policy-checks"),
                    h.Name("checks"),
                    h.Autocomplete("off"),
                    h.Placeholder("test, lint"),
                  ]),
                  switchRow(
                    "policyRequirePullRequest",
                    forms.policyRequirePullRequest,
                    "Require a pull request on protected branches",
                  ),
                  switchRow(
                    "policyRequireResolvedThreads",
                    forms.policyRequireResolvedThreads,
                    "Require review threads resolved before merge",
                  ),
                  h.div(
                    [h.Class("gp-dialog-actions")],
                    [
                      h.button(
                        [
                          h.Class("gp-btn-quiet"),
                          h.Type("submit"),
                          h.Disabled(model.settingsScreen.busy),
                        ],
                        ["Publish policy"],
                      ),
                    ],
                  ),
                ],
              ),
              h.div(
                [h.Class("gp-field-value"), h.DataAttribute("note", "policy")],
                [
                  rules.ref === null
                    ? "Defaults — nothing published yet."
                    : `Published at ${rules.ref.slice(0, 7)}.`,
                ],
              ),
            ],
          ),
      outcome(h, model, "policy"),
    ],
  );
};

const danger = (h: H): Html.Html =>
  h.section(
    [h.Class("gp-danger")],
    [
      h.h2([h.Class("gp-danger-title")], ["Danger zone"]),
      h.div(
        [h.Class("gp-danger-row")],
        [
          h.div(
            [],
            [
              h.div([h.Class("gp-danger-row-title")], ["Archive repository"]),
              h.div([h.Class("gp-danger-row-hint")], ["Mark read-only and hide from search."]),
            ],
          ),
          h.button(
            [
              h.Class("gp-danger-btn"),
              h.Type("button"),
              h.Disabled(true),
              h.Title("No API endpoint yet"),
            ],
            ["Archive"],
          ),
        ],
      ),
      h.div(
        [h.Class("gp-danger-row")],
        [
          h.div(
            [],
            [
              h.div([h.Class("gp-danger-row-title")], ["Delete repository"]),
              h.div([h.Class("gp-danger-row-hint")], ["Permanent. There is no undo."]),
            ],
          ),
          h.button(
            [
              h.Class("gp-danger-btn"),
              h.Type("button"),
              h.Disabled(true),
              h.Title("No API endpoint yet"),
            ],
            ["Delete"],
          ),
        ],
      ),
    ],
  );

export const view = (model: Model, h: H, ui: Elements<AppMessage>): Html.Html => {
  const held = AsyncData.getData(model.settingsScreen.data);
  const data = held._tag === "Some" ? held.value : null;
  return h.div(
    [h.Class("gp-screen")],
    [
      h.div(
        [h.Class("gp-settings")],
        [
          h.h1([h.Class("gp-heading")], ["Settings"]),
          general(h, model, data),
          identity(h, model),
          branches(h, model, data),
          tags(h, model, data),
          remotes(h, model, data),
          webhooks(h, model, data),
          maintenance(h, model, data),
          policy(h, ui, model, data),
          danger(h),
        ],
      ),
    ],
  );
};
