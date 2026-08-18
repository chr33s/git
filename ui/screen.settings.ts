/**
 * Settings.
 *
 * The merge-policy toggles are `ui-switch` from `@chr33s/base-wc`, authored the
 * native-first way its contract asks for: a real `<input type="checkbox">`
 * inside the element. The component overlays it and mirrors state onto
 * `data-state`, which `app.css` styles into the design's pill-and-knob — and
 * because the native input is the value, the form still submits with JS off.
 */
import { html, type TemplateResult } from "lit";
import { customElement } from "lit/decorators.js";

import { GitPlusElement } from "./base.ts";

interface Policy {
  readonly name: string;
  readonly label: string;
  readonly on: boolean;
}

const POLICIES: readonly Policy[] = [
  { name: "merge-commits", label: "Allow merge commits", on: true },
  { name: "squash", label: "Allow squash merging", on: true },
  { name: "rebase", label: "Allow rebase merging", on: false },
  { name: "require-checks", label: "Require passing checks before merge", on: true },
];

@customElement("gp-settings")
export class GpSettings extends GitPlusElement {
  protected override render(): TemplateResult {
    return html`
      <div class="gp-screen">
        <div class="gp-settings">
          <h1 class="gp-heading">Settings</h1>

          <section class="gp-setting-card">
            <h2 class="gp-setting-title">General</h2>
            <div class="gp-field-label">Repository name</div>
            <div class="gp-field-value">git-plus/core</div>
            <div class="gp-field-label">Default branch</div>
            <div class="gp-field-value">main</div>
          </section>

          <section class="gp-setting-card">
            <h2 class="gp-setting-title" data-with-hint>Merge policy</h2>
            <p class="gp-setting-hint">
              Which actions are allowed when a Change Request is mergeable.
            </p>
            <div class="gp-switch-list">
              ${POLICIES.map(
                (policy) => html`
                  <label class="gp-switch-row">
                    <ui-switch class="gp-switch">
                      <input type="checkbox" name=${policy.name} .defaultChecked=${policy.on} />
                      <span class="gp-switch-thumb"></span>
                    </ui-switch>
                    ${policy.label}
                  </label>
                `,
              )}
            </div>
          </section>

          <section class="gp-danger">
            <h2 class="gp-danger-title">Danger zone</h2>
            <div class="gp-danger-row">
              <div>
                <div class="gp-danger-row-title">Archive repository</div>
                <div class="gp-danger-row-hint">Mark read-only and hide from search.</div>
              </div>
              <button class="gp-danger-btn" type="button">Archive</button>
            </div>
            <div class="gp-danger-row">
              <div>
                <div class="gp-danger-row-title">Delete repository</div>
                <div class="gp-danger-row-hint">Permanent. There is no undo.</div>
              </div>
              <button class="gp-danger-btn" type="button">Delete</button>
            </div>
          </section>
        </div>
      </div>
    `;
  }
}

declare global {
  interface HTMLElementTagNameMap {
    "gp-settings": GpSettings;
  }
}
