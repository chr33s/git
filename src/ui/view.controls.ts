import { Dialog } from "@foldkit/ui";
import { Html } from "foldkit";

import { AppMessage } from "./app.message.ts";
import type { Model } from "./app.model.ts";
import type { DialogName } from "./controls.ts";

/** The caller owns the form and styling; Dialog owns modality and focus. */
export const modal = (
  h: Html.HtmlBuilder<AppMessage>,
  model: Model,
  name: DialogName,
  className: string,
  trigger: Html.Html,
  content: (render: Dialog.RenderInfo) => ReadonlyArray<Html.Html | string>,
): Html.Html =>
  h.div(
    [h.Class(className)],
    [
      trigger,
      h.submodel({
        slotId: name,
        model: model.controls.dialogs[name],
        view: Dialog.view,
        toParentMessage: (message) => AppMessage.GotDialogMessage({ dialogId: name, message }),
        viewInputs: {
          hasDescription: true,
          toView: (render) =>
            h.dialog(
              [...render.dialog, h.Class("gp-dialog-root")],
              render.isVisible
                ? [
                    h.div([...render.backdrop, h.Class("gp-dialog-backdrop")]),
                    h.div([...render.panel, h.Class("gp-dialog")], content(render)),
                  ]
                : [],
            ),
        },
      }),
    ],
  );
