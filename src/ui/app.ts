/**
 * The application, assembled.
 *
 * `makeApplication` rather than `makeElement`: this owns the document. The
 * routing config is what makes the address bar part of the Model — the runtime
 * turns `popstate`, an intercepted link click and a `pushUrl` of our own into
 * one `ChangedUrl`, so there is exactly one path from an address to a screen.
 *
 * `init` reads the world once — the address, the stored palette, the stored
 * rail width — and everything after that arrives as a Message. The two things
 * it starts are the two the page cannot draw itself: who is asking, and what
 * the repository's hub holds.
 */
import { AsyncData, Runtime, Update, Url } from "foldkit";

import { AppMessage } from "./app.message.ts";
import { Model } from "./app.model.ts";
import { routeOfUrl } from "./app.route.ts";
import { update } from "./app.update.ts";
import { view } from "./app.view.ts";
import { subscriptions } from "./app.subscription.ts";
import { FetchIdentity, LoadCommits, LoadTasks, seedSessions, seedTasks } from "./app.command.ts";
import { railCollapsed } from "./app.command.shell.ts";
import { LoadBrowserKey, LoadSettings } from "./app.command.settings.ts";
import { LoadCode, OpenLocalRepository } from "./app.command.code.ts";
import { HydrateDetail } from "./app.command.detail.ts";
import { repoFromDocument } from "./client.ts";
import { clientFromDocument } from "./api.ts";
import * as palette from "./theme.ts";

const init = (url: Url.Url): Update.Return<Model, AppMessage> => {
  const route = routeOfUrl(url);
  return {
    model: {
      route,
      repo: repoFromDocument(),
      cloneUrl: clientFromDocument().cloneUrl,
      theme: palette.current(),
      themePinned: palette.stored() !== null,
      viewer: AsyncData.Loading(),
      localRepository: "Unavailable",
      query: "",
      // A cold load on a malformed address explains itself too, not only one
      // reached by a link: the address bar is the one place a reader types.
      navError: route._tag === "NotFound" ? "this link's address is malformed" : null,
      railCollapsed: railCollapsed(),
      tasks: {
        // The design's sample data, which is what an offline page shows. The
        // hub replaces it wholesale when it answers with anything.
        tasks: seedTasks,
        sessions: seedSessions,
        liveNotice: null,
        load: AsyncData.Loading(),
      },
      tasksScreen: { filter: "all", title: "", desc: "", parent: "", submitting: false },
      searchScreen: { code: AsyncData.Idle() },
      activityScreen: { zoom: "week", offset: 0, commits: AsyncData.Loading(), wanted: 0 },
      detailScreen: {
        tab: "conversation",
        // Seeded from the address: a cold load on a Change Request is opening
        // it, exactly as a click would be, and a diff that did not know whose
        // it was would wait for an answer it had already been given.
        diffFor: route._tag === "Detail" ? route.id : null,
        diff: AsyncData.Idle(),
        comment: "",
        replies: {},
        acting: false,
        notice: null,
        taskNotice: null,
        moveNotice: null,
      },
      codeScreen: {
        view: AsyncData.Loading(),
        wantedRef: "",
        mode: "view",
        newPath: null,
        draft: "",
        message: "",
        saving: false,
        editError: null,
        panel: "none",
        history: AsyncData.Idle(),
        at: null,
        session: 0,
        diffing: false,
        copied: false,
        sync: null,
        syncing: false,
        syncNotice: null,
        newBranch: "",
        proposeTitle: "",
        proposeDescription: "",
        bisect: null,
      },
      settingsScreen: {
        data: AsyncData.Loading(),
        browserKey: null,
        notes: {},
        busy: false,
        policyPublished: null,
        reflog: null,
        forms: {
          resetRef: "",
          resetTo: "",
          tagName: "",
          tagMessage: "",
          remoteName: "",
          remoteUrl: "",
          remoteCredential: "",
          webhookUrl: "",
          webhookSecret: "",
          policyProtected: "",
          policyApprovals: "0",
          policyChecks: "",
          policyRequirePullRequest: false,
          policyRequireResolvedThreads: false,
        },
      },
    },
    commands: [
      FetchIdentity(),
      LoadTasks(),
      LoadCommits({ wanted: 0 }),
      LoadSettings(),
      LoadBrowserKey(),
      // The address may already name a file; opening it is what makes a deep
      // link land on that file rather than on the README.
      LoadCode({ ref: "", keep: route._tag === "Code" ? route.path : "" }),
      // Off the boot path: the clone brings the pack machinery and the Effect
      // runtime with it, and first paint should not wait on either.
      OpenLocalRepository({ subject: null }),
      // A cold load on a Change Request fills in what a listing row omits,
      // exactly as opening one from the list does.
      ...(route._tag === "Detail" ? [HydrateDetail({ id: route.id, head: null })] : []),
    ],
  };
};

export const application = Runtime.makeApplication({
  Model,
  init,
  update,
  view,
  subscriptions,
  routing: {
    onUrlChange: (url) => AppMessage.ChangedUrl({ route: routeOfUrl(url) }),
    onUrlRequest: (request) => AppMessage.RequestedUrl({ request }),
  },
  container: document.querySelector("#gp-app"),
  devTools: { show: "Development" },
});
