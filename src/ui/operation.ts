/**
 * Reusable operation-progress view.
 *
 * Used by maintenance, remotes, and any later server operation. The element
 * is presentational: the caller feeds it an operation snapshot (and optional
 * retry callback) rather than talking to the API itself.
 */
import { html, nothing, type TemplateResult } from "lit";
import { customElement, property } from "lit/decorators.js";

import { GitPlusElement } from "./base.ts";

export interface OperationProgressView {
  readonly id: string;
  readonly kind: string;
  readonly state: "queued" | "running" | "succeeded" | "failed" | "cancelled";
  readonly message?: string;
  readonly progress?: {
    readonly current?: number;
    readonly total?: number;
    readonly unit?: string;
  };
  readonly error?: { readonly tag: string; readonly message: string };
}

@customElement("gp-operation")
export class GpOperation extends GitPlusElement {
  @property({ attribute: false }) accessor operation: OperationProgressView | null = null;
  @property({ attribute: false }) accessor onRetry: (() => void) | null = null;

  protected override render(): TemplateResult {
    const operation = this.operation;
    if (operation === null) return html`${nothing}`;

    const percent = percentOf(operation);
    const failed = operation.state === "failed" || operation.state === "cancelled";
    const done = operation.state === "succeeded";

    return html`
      <section class="gp-card gp-operation" data-state=${operation.state}>
        <header class="gp-card-head">
          <span>${operation.kind}</span>
          <span>${operation.state}</span>
        </header>
        <div class="gp-operation-body">
          ${operation.message === undefined ? nothing : html`<p>${operation.message}</p>`}
          ${percent === null ? nothing : html`<progress max="100" value=${percent}></progress>`}
          ${
            operation.error === undefined
              ? nothing
              : html`<p class="gp-operation-error">
                  ${operation.error.tag}: ${operation.error.message}
                </p>`
          }
          ${
            failed && this.onRetry !== null
              ? html`<button class="gp-btn-primary" type="button" @click=${this.onRetry}>
                  Retry
                </button>`
              : nothing
          }
          ${done ? html`<p>complete</p>` : nothing}
        </div>
      </section>
    `;
  }
}

const percentOf = (operation: OperationProgressView): number | null => {
  const progress = operation.progress;
  if (progress?.current === undefined) return null;
  if (progress.total !== undefined && progress.total > 0) {
    return Math.min(100, Math.round((progress.current / progress.total) * 100));
  }
  return Math.min(100, Math.max(0, progress.current));
};
