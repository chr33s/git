import { Dialog, Menu, RadioGroup, Tabs } from "@foldkit/ui";
import { Option } from "effect";
import { Update } from "foldkit";

import type { Model } from "./app.model.ts";
import { AppMessage } from "./app.message.ts";
import {
  ActionMenu,
  DetailTabs,
  TaskFilter,
  TimelineZoom,
  type DialogName,
  type MenuName,
} from "./controls.ts";

type Return = Update.Return<Model, AppMessage>;

export const foldDialog = (model: Model, name: DialogName, message: Dialog.Message): Return =>
  Update.foldChild({
    update: Dialog.update,
    read: (model: Model) => Option.some(model.controls.dialogs[name]),
    write: (model, dialog) => ({
      ...model,
      controls: { ...model.controls, dialogs: { ...model.controls.dialogs, [name]: dialog } },
    }),
    toParentMessage: (message) => AppMessage.GotDialogMessage({ dialogId: name, message }),
    foldOutMessage: () => (model: Model) => ({ model }),
  })(model, message);

/** Fold the child's public capability without constructing its messages. */
export const changeDialog = (model: Model, name: DialogName, change: typeof Dialog.open): Return =>
  Update.foldChildStep({
    update: change,
    read: (model: Model) => Option.some(model.controls.dialogs[name]),
    write: (model, dialog) => ({
      ...model,
      controls: { ...model.controls, dialogs: { ...model.controls.dialogs, [name]: dialog } },
    }),
    toParentMessage: (message) => AppMessage.GotDialogMessage({ dialogId: name, message }),
    foldOutMessage: () => (model: Model) => ({ model }),
  })(model);

/** Close in the same transition as a successful submit or cancellation. */
export const closeDialog = (result: Return, name: DialogName): Return => {
  const closed = changeDialog(result.model, name, Dialog.close);
  return {
    model: closed.model,
    commands: [...(result.commands ?? []), ...(closed.commands ?? [])],
  };
};

export const foldMenu = (
  model: Model,
  name: MenuName,
  message: Menu.Message,
  selected: (model: Model, item: string) => Return,
): Return =>
  Update.foldChild({
    update: ActionMenu.update,
    read: (model: Model) => Option.some(model.controls.menus[name]),
    write: (model, menu) => ({
      ...model,
      controls: { ...model.controls, menus: { ...model.controls.menus, [name]: menu } },
    }),
    toParentMessage: (message) => AppMessage.GotMenuMessage({ menuId: name, message }),
    foldOutMessage: (out: Menu.OutMessage) => (model: Model) => selected(model, out.value),
  })(model, message);

export const foldDetailTabs = (
  model: Model,
  message: Tabs.Message,
  selected: (model: Model, value: "conversation" | "diff" | "commits" | "checks") => Return,
): Return =>
  Update.foldChild({
    update: DetailTabs.update,
    read: (model: Model) => Option.some(model.controls.detailTabs),
    write: (model, detailTabs) => ({ ...model, controls: { ...model.controls, detailTabs } }),
    toParentMessage: (message) => AppMessage.GotDetailTabsMessage({ message }),
    foldOutMessage:
      (out: Tabs.OutMessage<"conversation" | "diff" | "commits" | "checks">) => (model: Model) =>
        selected(model, out.value),
  })(model, message);

export const foldTaskFilter = (
  model: Model,
  message: RadioGroup.Message,
  selected: (model: Model, value: "all" | "tasks" | "crs") => Return,
): Return =>
  Update.foldChild({
    update: TaskFilter.update,
    read: (model: Model) => Option.some(model.controls.taskFilter),
    write: (model, taskFilter) => ({ ...model, controls: { ...model.controls, taskFilter } }),
    toParentMessage: (message) => AppMessage.GotTaskFilterMessage({ message }),
    foldOutMessage: (out: RadioGroup.OutMessage<"all" | "tasks" | "crs">) => (model: Model) =>
      selected(model, out.value),
  })(model, message);

export const foldTimelineZoom = (
  model: Model,
  message: RadioGroup.Message,
  selected: (model: Model, value: "day" | "week" | "month") => Return,
): Return =>
  Update.foldChild({
    update: TimelineZoom.update,
    read: (model: Model) => Option.some(model.controls.timelineZoom),
    write: (model, timelineZoom) => ({ ...model, controls: { ...model.controls, timelineZoom } }),
    toParentMessage: (message) => AppMessage.GotTimelineZoomMessage({ message }),
    foldOutMessage: (out: RadioGroup.OutMessage<"day" | "week" | "month">) => (model: Model) =>
      selected(model, out.value),
  })(model, message);
