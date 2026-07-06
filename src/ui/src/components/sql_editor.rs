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
//     -> DisposableClosure<...>` — HOLD the returned value (drop = unsubscribe). [Task 4]
//   - Completion [Task 5]: `monaco::sys::languages::register_completion_item_provider(
//     language_id: &str, provider: &CompletionItemProvider) -> IDisposable`;
//     `CompletionItemKind` enum in `monaco::sys::languages`; offset via the sys
//     `ITextModel::get_offset_at(&Position)` (reachable through `model.as_ref()`);
//     `Position` has `line_number()`/`column()` (`monaco::sys::Position`).
//   - Build: monaco ships its JS vendored inside the crate via wasm-bindgen module
//     snippets — wasm-bindgen `--target web --out-dir $OUT` emits `$OUT/snippets/`,
//     which the genrule's `out=dist` already captures. No CDN, no extra copy.

use monaco::api::{CodeEditor, CodeEditorOptions};
use monaco::sys::editor::BuiltinTheme;
use stylist::yew::styled_component;
use web_sys::HtmlElement;
use yew::prelude::*;

/// Reusable Monaco-backed SQL editor. Minimal (empty) in this spike; controlled
/// props and schema-fed completion are added in later tasks.
#[styled_component(SqlEditor)]
pub fn sql_editor() -> Html {
    let node = use_node_ref();
    // Keep the editor alive for the component's lifetime; dropping it on unmount
    // triggers `CodeEditor`'s `Drop`, which disposes the underlying JS editor.
    let editor = use_mut_ref(|| None::<CodeEditor>);

    {
        let editor = editor.clone();
        use_effect_with(node.clone(), move |node| {
            let el: HtmlElement = node.cast().expect("sql-editor node is an HtmlElement");
            let opts = CodeEditorOptions::default()
                .with_language("sql".to_owned())
                .with_value("SELECT 1\n".to_owned())
                .with_builtin_theme(BuiltinTheme::VsDark)
                .with_automatic_layout(true);
            *editor.borrow_mut() = Some(CodeEditor::create(&el, Some(opts)));
            move || {
                editor.borrow_mut().take();
            }
        });
    }

    let css = css!(
        r"
        .sqled-host { width: 100%; height: 320px; border: 1px solid var(--loom-border); border-radius: var(--loom-radius); overflow: hidden; }
        "
    );
    html! { <div class={css}><div ref={node} class="sqled-host" /></div> }
}
