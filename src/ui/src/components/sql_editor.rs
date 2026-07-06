// SPIKE NOTES (monaco 0.7.0, confirmed against the vendored crate source) —
// the exact API surface later tasks (4/5/6) reconcile against:
//   - `monaco::api::CodeEditor::create(&web_sys::HtmlElement, Some(opts)) -> CodeEditor`
//     where opts: Into<IStandaloneEditorConstructionOptions> (CodeEditorOptions is).
//   - `CodeEditorOptions::default()` builder: `.with_language(String)`,
//     `.with_value(String)`, `.with_model(TextModel)`, `.with_builtin_theme(BuiltinTheme)`,
//     `.with_automatic_layout(bool)`, `.with_theme(String)`.
//   - `CodeEditor::get_model() -> Option<TextModel>`; `CodeEditor` disposes on Drop
//     (so dropping the stored editor on unmount is the dispose).
//   - `monaco::api::TextModel::create(value, Some("sql"), None) -> Result<TextModel, JsValue>`;
//     `TextModel::get_value() -> String`; `TextModel::set_value(&str)`.
//   - `CodeEditor::on_did_change_model_content(FnMut(IModelContentChangedEvent))
//     -> DisposableClosure<dyn FnMut(IModelContentChangedEvent)>` — HOLD the returned
//     value (drop = unsubscribe). Confirmed via the `event_methods!` macro
//     (`src/api/macros.rs`) that backs every `on_did_*` method on `CodeEditor`. [Task 4]
//   - read_only [Task 4]: `CodeEditorOptions` has no `read_only` field. Applied
//     post-creation via `IStandaloneCodeEditor::update_options_editor(&IEditorOptions)`
//     (`monaco::sys::editor`), where `IEditorOptions::set_read_only(Option<bool>)`
//     exists but `IEditorOptions` itself has no generated `Default` impl (it's absent
//     from the crate's `impl_default_empty_obj!` list) — build one the same way the
//     crate does internally, via `js_sys::Object::new().unchecked_into()`.
//   - Completion [Task 5]: `monaco::sys::languages::register_completion_item_provider(
//     language_id: &str, provider: &CompletionItemProvider) -> IDisposable`;
//     `CompletionItemKind` enum in `monaco::sys::languages`; offset via the sys
//     `ITextModel::get_offset_at(&Position)` (reachable through `model.as_ref()`);
//     `Position` has `line_number()`/`column()` (`monaco::sys::Position`).
//   - Build: monaco ships its JS vendored inside the crate via wasm-bindgen module
//     snippets — wasm-bindgen `--target web --out-dir $OUT` emits `$OUT/snippets/`,
//     which the genrule's `out=dist` already captures. No CDN, no extra copy.

use loom_ui_core::CompletionSchema;
use monaco::api::{CodeEditor, CodeEditorOptions, DisposableClosure, TextModel};
use monaco::sys::editor::{BuiltinTheme, IEditorOptions, IModelContentChangedEvent};
use stylist::yew::styled_component;
use wasm_bindgen::JsCast;
use web_sys::HtmlElement;
use yew::prelude::*;

#[derive(Properties, PartialEq)]
pub struct SqlEditorProps {
    /// Current SQL text (controlled).
    pub value: AttrValue,
    /// Fired whenever the editor content changes.
    pub on_change: Callback<String>,
    /// Completion source (used by Task 5's provider).
    #[prop_or_default]
    pub schema: CompletionSchema,
    #[prop_or_default]
    pub read_only: bool,
    /// CSS height, e.g. "320px". Defaults to 320px.
    #[prop_or_default]
    pub height: Option<AttrValue>,
}

/// Reusable Monaco-backed SQL editor. Controlled: the caller owns the text via
/// `value`/`on_change`; schema-fed completion is added in Task 5.
#[styled_component(SqlEditor)]
pub fn sql_editor(props: &SqlEditorProps) -> Html {
    let node = use_node_ref();
    // Keep the editor alive for the component's lifetime; dropping it on unmount
    // triggers `CodeEditor`'s `Drop`, which disposes the underlying JS editor.
    let editor = use_mut_ref(|| None::<CodeEditor>);
    // Hold the on_did_change subscription so it isn't dropped (which would
    // unsubscribe) for as long as the editor lives.
    let subscription =
        use_mut_ref(|| None::<DisposableClosure<dyn FnMut(IModelContentChangedEvent)>>);
    // Hold the Monaco text model created via `with_model` so it can be disposed
    // explicitly on unmount — `CodeEditor::Drop` only disposes the editor widget,
    // not a model supplied this way, and `TextModel` itself has no `Drop`.
    let model_ref = use_mut_ref(|| None::<TextModel>);

    // Mount: create the editor, seed the model from the initial `value`, and
    // subscribe to content changes. Schema/read_only are captured by value so
    // this effect only reruns when `node` changes (i.e. once, on mount). Because
    // of this, a caller that swaps `on_change` or flips `read_only` after mount
    // won't see the change take effect — a known limitation for later tasks/callers.
    {
        let editor = editor.clone();
        let subscription = subscription.clone();
        let model_ref = model_ref.clone();
        let on_change = props.on_change.clone();
        let initial = props.value.to_string();
        let read_only = props.read_only;
        use_effect_with(node.clone(), move |node| {
            let el: HtmlElement = node.cast().expect("sql-editor node is an HtmlElement");
            let model =
                TextModel::create(&initial, Some("sql"), None).expect("create SQL text model");
            let opts = CodeEditorOptions::default()
                .with_language("sql".to_owned())
                .with_builtin_theme(BuiltinTheme::VsDark)
                .with_automatic_layout(true)
                .with_model(model.clone());
            let ed = CodeEditor::create(&el, Some(opts));

            if read_only {
                let ro_opts: IEditorOptions = js_sys::Object::new().unchecked_into();
                ro_opts.set_read_only(Some(true));
                ed.as_ref().update_options_editor(&ro_opts);
            }

            // Emit on_change with the model's current text on every edit.
            let cb_model = model.clone();
            let disposable = ed.on_did_change_model_content(move |_ev| {
                on_change.emit(cb_model.get_value());
            });
            *subscription.borrow_mut() = Some(disposable);
            *editor.borrow_mut() = Some(ed);
            *model_ref.borrow_mut() = Some(model);

            move || {
                subscription.borrow_mut().take(); // unsubscribe
                editor.borrow_mut().take(); // dispose editor widget
                if let Some(model) = model_ref.borrow_mut().take() {
                    model.as_ref().dispose(); // dispose the model (editor.dispose() does not)
                }
            }
        });
    }

    // Controlled-with-guard: only force-set the model when the incoming prop
    // differs from the editor's current text (avoids fighting the buffer/cursor
    // during normal typing, where `on_change` already updated the caller's state
    // to match what the editor already holds).
    {
        let editor = editor.clone();
        use_effect_with(props.value.clone(), move |value| {
            if let Some(model) = editor.borrow().as_ref().and_then(CodeEditor::get_model)
                && model.get_value() != *value
            {
                model.set_value(value);
            }
            || ()
        });
    }

    let h = props
        .height
        .as_ref()
        .map_or_else(|| "320px".to_owned(), std::string::ToString::to_string);
    let css = css!(
        r#"
        .sqled-host { width: 100%; border: 1px solid var(--loom-border); border-radius: var(--loom-radius); overflow: hidden; }
        "#
    );
    html! {
        <div class={css}>
            <div ref={node} class="sqled-host" style={format!("height:{h};")} />
        </div>
    }
}
