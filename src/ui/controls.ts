import { Dialog, Menu, RadioGroup, Tabs } from "@foldkit/ui";
import { Schema } from "effect";

export const DialogName = Schema.Literals(["newTask", "newBranch", "clone", "propose"]);
export type DialogName = typeof DialogName.Type;
export const MenuName = Schema.Literals(["explorer", "branch"]);
export type MenuName = typeof MenuName.Type;

export const DetailTabs = Tabs.create<"conversation" | "diff" | "commits" | "checks">();
export const TaskFilter = RadioGroup.create<"all" | "tasks" | "crs">();
export const TimelineZoom = RadioGroup.create<"day" | "week" | "month">();
export const ActionMenu = Menu.create<string>();

export const Model = Schema.Struct({
  searchVersion: Schema.Number,
  dialogs: Schema.Struct({
    newTask: Dialog.Model,
    newBranch: Dialog.Model,
    clone: Dialog.Model,
    propose: Dialog.Model,
  }),
  menus: Schema.Struct({ explorer: Menu.Model, branch: Menu.Model }),
  detailTabs: Tabs.Model,
  taskFilter: RadioGroup.Model,
  timelineZoom: RadioGroup.Model,
});
export type Model = typeof Model.Type;

export const init = (): Model => ({
  searchVersion: 0,
  dialogs: {
    newTask: Dialog.init({ id: "gp-new-task-dialog", focusSelector: "#gp-new-title" }),
    newBranch: Dialog.init({ id: "gp-new-branch-dialog", focusSelector: "#gp-new-branch-name" }),
    clone: Dialog.init({ id: "gp-clone-dialog" }),
    propose: Dialog.init({ id: "gp-propose-dialog", focusSelector: "#gp-propose-title" }),
  },
  menus: {
    explorer: Menu.init({ id: "gp-explorer-menu", isModal: false }),
    branch: Menu.init({ id: "gp-branch-menu", isModal: false }),
  },
  detailTabs: Tabs.init({ id: "gp-detail-tabs" }),
  taskFilter: RadioGroup.init({ id: "gp-task-filter" }),
  timelineZoom: RadioGroup.init({ id: "gp-timeline-zoom" }),
});
