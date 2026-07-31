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

use loom_ui_core::{
    CompletionSchema, Diagnostic, DiagnosticSeverity, LOOM_BG, LOOM_TEXT, Suggestion,
    SuggestionKind, cursor_context, sql_completions, sql_diagnostics,
};
use monaco::api::{CodeEditor, CodeEditorOptions, DisposableClosure, TextModel};
use monaco::sys::editor::{
    BuiltinTheme, IEditorOptions, IMarkerData, IModelContentChangedEvent, IStandaloneThemeData,
    ITextModel, set_model_markers,
};
use monaco::sys::languages::CompletionItemProvider;
use monaco::sys::{IDisposable, MarkerSeverity, Position};
use stylist::yew::styled_component;
use wasm_bindgen::closure::Closure;
use wasm_bindgen::{JsCast, JsValue};
use web_sys::HtmlElement;
use yew::prelude::*;

/// A request for server-side validation of the editor's current text. The editor emits
/// this on the debounce; the caller performs the (async) fetch and answers through
/// `respond`. Structured this way because Yew callbacks are synchronous — the editor
/// owns *when* to ask and *what to do with the answer*, the caller owns the transport.
pub struct ValidateRequest {
    pub sql: String,
    pub respond: Callback<ValidateResponse>,
}

/// The answer to a [`ValidateRequest`]. `sql` echoes the text that was validated so the
/// editor can drop a response whose text the user has already edited past — without it,
/// markers flicker onto text they do not describe.
pub struct ValidateResponse {
    pub sql: String,
    pub diagnostics: Vec<Diagnostic>,
}

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
    /// Optional server-side validation source. When `None` the editor behaves exactly as
    /// before — only the client-side `sql_diagnostics` engine publishes markers — so the
    /// Transforms drawer editors are unaffected and only the SQL console opts in.
    #[prop_or_default]
    pub validate: Option<Callback<ValidateRequest>>,
}

/// Marker owner for the client-side `sql_diagnostics` engine.
const CLIENT_OWNER: &str = "loom";
/// Marker owner for server-side (`POST /sql/validate`) diagnostics. Distinct from
/// [`CLIENT_OWNER`] so `set_model_markers` replaces only this source's set: the two
/// engines publish independently and neither clears the other's squiggles.
const SERVER_OWNER: &str = "loom-server";

/// How long the editor waits after the last keystroke before asking the server. Long
/// enough that ordinary typing does not generate a request per character, short enough
/// that a pause feels answered.
const VALIDATE_DEBOUNCE_MS: i32 = 500;

/// The in-flight debounce: the pending `setTimeout` id plus the closure it will run.
/// The closure MUST be held for as long as the timeout is armed (dropping it
/// invalidates the JS callback), and the id is what cancels it.
type PendingValidation = std::rc::Rc<std::cell::RefCell<Option<(i32, Closure<dyn FnMut()>)>>>;

/// The mounted editor's Monaco model, or `None` once the cleanup has taken it. Shared
/// with any armed or in-flight validation so both can tell that the editor is gone —
/// `TextModel` has no "is disposed" query, and calling into a disposed one throws.
type LiveModel = std::rc::Rc<std::cell::RefCell<Option<TextModel>>>;

/// Build the Monaco marker array for `diags`. Shared by both publishers so the
/// severity/position mapping exists once.
fn marker_array(diags: &[Diagnostic]) -> js_sys::Array {
    let markers = js_sys::Array::new();
    for d in diags {
        let m: IMarkerData = js_sys::Object::new().unchecked_into();
        m.set_message(&d.message);
        m.set_severity(match d.severity {
            DiagnosticSeverity::Warning => MarkerSeverity::Warning,
            DiagnosticSeverity::Error => MarkerSeverity::Error,
        });
        m.set_start_line_number(f64::from(d.line));
        m.set_start_column(f64::from(d.start_col));
        m.set_end_line_number(f64::from(d.line));
        m.set_end_column(f64::from(d.end_col));
        markers.push(&m);
    }
    markers
}

/// Recompute client-side SQL diagnostics for the model's current text and publish
/// them as Monaco markers under the `loom` owner (replacing any prior `loom`
/// markers on the model). Called on mount and on every content change; an empty
/// result clears the squiggles. The pure engine lives in `loom_ui_core`.
fn refresh_diagnostics(model: &TextModel, schema: &CompletionSchema) {
    let diags: Vec<Diagnostic> = sql_diagnostics(schema, &model.get_value());
    set_model_markers(model.as_ref(), CLIENT_OWNER, &marker_array(&diags));
}

/// Publish server-side diagnostics under [`SERVER_OWNER`], replacing that source's prior
/// set (an empty slice therefore clears them). Never touches the client engine's markers.
fn publish_server_diagnostics(model: &TextModel, diags: &[Diagnostic]) {
    set_model_markers(model.as_ref(), SERVER_OWNER, &marker_array(diags));
}

/// Cancel any armed validation debounce, dropping its closure. Idempotent: safe to call
/// when nothing is pending, and safe to call on unmount — which is the point, since a
/// timeout that survived unmount would fire against a disposed model.
fn cancel_pending_validation(pending: &PendingValidation) {
    if let Some((handle, _closure)) = pending.borrow_mut().take()
        && let Some(window) = web_sys::window()
    {
        window.clear_timeout_with_handle(handle);
    }
}

/// Ask `validate` for server diagnostics on the live model's current text,
/// `VALIDATE_DEBOUNCE_MS` after the last call. Any previously scheduled request is
/// cancelled, so a burst of keystrokes produces exactly one request. The response is
/// dropped unless its `sql` still matches the model — otherwise markers land on text the
/// user has already edited past.
///
/// Takes the model through the shared [`LiveModel`] slot rather than by reference,
/// because a request already in flight can outlive the component: the cleanup takes the
/// model out of the slot before disposing it, so both the request and the response see
/// `None` after unmount and do nothing. Holding a `TextModel` clone instead would let a
/// late response call `get_value()` on a disposed model, which throws.
fn schedule_validation(
    live: &LiveModel,
    validate: &Callback<ValidateRequest>,
    pending: &PendingValidation,
) {
    let Some(window) = web_sys::window() else {
        return;
    };
    // Debounce cancellation: whatever was armed by the previous keystroke is disarmed
    // (and its closure dropped) before a new timeout replaces it.
    cancel_pending_validation(pending);
    let live_for_request = live.clone();
    let validate = validate.clone();
    let fire = Closure::wrap(Box::new(move || {
        // Unmounted between arming and firing: nothing to validate.
        let Some(sql) = live_for_request.borrow().as_ref().map(TextModel::get_value) else {
            return;
        };
        let live_for_response = live_for_request.clone();
        let respond = Callback::from(move |resp: ValidateResponse| {
            // Unmounted while the request was in flight: the model is gone and touching
            // it would throw. Nothing to publish onto, so drop the answer.
            let live = live_for_response.borrow();
            let Some(model) = live.as_ref() else {
                return;
            };
            // Stale-response drop: the answer describes the text that was sent, and the
            // user may have typed since. Publishing it would squiggle text it does not
            // describe, so a response whose `sql` no longer matches the live model is
            // discarded outright.
            if model.get_value() == resp.sql {
                publish_server_diagnostics(model, &resp.diagnostics);
            }
        });
        validate.emit(ValidateRequest { sql, respond });
    }) as Box<dyn FnMut()>);
    if let Ok(handle) = window.set_timeout_with_callback_and_timeout_and_arguments_0(
        fire.as_ref().unchecked_ref(),
        VALIDATE_DEBOUNCE_MS,
    ) {
        // Holds the closure alive until the timeout fires or the next keystroke replaces
        // it; a fired-but-unreplaced entry is harmless (clearing a spent handle is a no-op).
        *pending.borrow_mut() = Some((handle, fire));
    }
}

/// The completion provider's closure AND its registration, held together for the
/// editor's life: dropping the `Closure` invalidates the JS callback, and disposing the
/// `IDisposable` unregisters the provider.
type CompletionRegistration = (
    Closure<dyn FnMut(ITextModel, Position) -> JsValue>,
    IDisposable,
);

/// Define the `loom-dark` Monaco theme (mirrors the `--loom-*` palette tokens from
/// `global.rs`). Monaco tolerates redefinition, so this runs on every mount rather than
/// behind a "define once" flag.
///
/// Monaco cannot read CSS custom properties, so the editor chrome draws its colors from
/// the same `loom_ui_core` palette constants that back the `--loom-bg`/`--loom-text`
/// tokens — one source of truth.
fn define_loom_theme() {
    let theme_data: IStandaloneThemeData = js_sys::Object::new().unchecked_into();
    theme_data.set_base(BuiltinTheme::VsDark);
    theme_data.set_inherit(true);
    theme_data.set_rules(&js_sys::Array::new());
    let colors = js_sys::Object::new();
    js_sys::Reflect::set(&colors, &"editor.background".into(), &LOOM_BG.into())
        .expect("set editor.background");
    js_sys::Reflect::set(&colors, &"editor.foreground".into(), &LOOM_TEXT.into())
        .expect("set editor.foreground");
    theme_data.set_colors(&colors);
    monaco::sys::editor::define_theme("loom-dark", &theme_data).expect("define loom-dark theme");
}

/// The replace range every suggestion on a line shares — the word under the cursor.
/// Modern Monaco silently drops range-less completion items, so this is mandatory.
fn suggestion_range(line: f64, start_column: f64, end_column: f64) -> js_sys::Object {
    let range = js_sys::Object::new();
    for (key, value) in [
        ("startLineNumber", line),
        ("endLineNumber", line),
        ("startColumn", start_column),
        ("endColumn", end_column),
    ] {
        js_sys::Reflect::set(&range, &key.into(), &JsValue::from_f64(value))
            .expect("set suggestion range field");
    }
    range
}

/// One Monaco completion item built from a `loom_ui_core` suggestion.
fn suggestion_item(s: Suggestion, range: &js_sys::Object) -> js_sys::Object {
    let kind = match s.kind {
        SuggestionKind::Keyword => 17.0, // CompletionItemKind.Keyword
        SuggestionKind::Table => 5.0,    // CompletionItemKind.Class
        SuggestionKind::Column => 3.0,   // CompletionItemKind.Field
    };
    let item = js_sys::Object::new();
    js_sys::Reflect::set(&item, &"label".into(), &s.label.into()).expect("set label");
    js_sys::Reflect::set(&item, &"kind".into(), &JsValue::from_f64(kind)).expect("set kind");
    js_sys::Reflect::set(&item, &"insertText".into(), &s.insert_text.into())
        .expect("set insertText");
    if let Some(detail) = s.detail {
        js_sys::Reflect::set(&item, &"detail".into(), &detail.into()).expect("set detail");
    }
    js_sys::Reflect::set(&item, &"range".into(), range).expect("set range");
    item
}

/// Register one `sql` `CompletionItemProvider` driven by the pure engine over `schema`,
/// returning the closure + registration the caller must hold for the editor's life.
///
/// KNOWN LIMITATION (v1): the provider is registered per mounted editor capturing that
/// editor's schema, so two concurrently-mounted `SqlEditor`s would register two `sql`
/// providers. Acceptable — the current surfaces mount exactly one; multi-editor
/// de-duplication is a follow-up.
fn register_completion(schema: CompletionSchema) -> CompletionRegistration {
    let provider: CompletionItemProvider = js_sys::Object::new().unchecked_into();
    let cb = Closure::wrap(
        Box::new(move |model: ITextModel, position: Position| -> JsValue {
            let text = model.get_value(None, None);
            let offset = model.get_offset_at(position.unchecked_ref()) as usize;
            let (prefix, qualifier) = cursor_context(&text, offset);
            let items = sql_completions(&schema, &prefix, qualifier.as_deref());

            let word = model.get_word_until_position(position.unchecked_ref());
            let range = suggestion_range(
                position.line_number(),
                word.start_column(),
                word.end_column(),
            );

            let suggestions = js_sys::Array::new();
            for s in items {
                suggestions.push(&suggestion_item(s, &range));
            }
            let list = js_sys::Object::new();
            js_sys::Reflect::set(&list, &"suggestions".into(), &suggestions)
                .expect("set suggestions");
            list.into()
        }) as Box<dyn FnMut(ITextModel, Position) -> JsValue>,
    );
    js_sys::Reflect::set(
        &provider,
        &"provideCompletionItems".into(),
        cb.as_ref().unchecked_ref(),
    )
    .expect("set provideCompletionItems");
    let reg = monaco::sys::languages::register_completion_item_provider("sql", &provider);
    (cb, reg)
}

/// Reusable Monaco-backed SQL editor. Controlled: the caller owns the text via
/// `value`/`on_change`, with schema-fed completion and both diagnostic sources wired in.
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
    // Hold the completion provider's closure AND its registration for the
    // editor's life: dropping the `Closure` invalidates the JS callback, and
    // dropping/disposing the `IDisposable` unregisters the provider.
    let completion = use_mut_ref(|| None::<CompletionRegistration>);

    // Mount: create the editor, seed the model from the initial `value`, and
    // subscribe to content changes. Schema/read_only/validate are captured by value
    // so this effect only reruns when `node` changes (i.e. once, on mount). Because
    // of this, a caller that swaps `on_change`/`validate` or flips `read_only` after
    // mount won't see the change take effect — a known limitation for callers.
    {
        let editor = editor.clone();
        let subscription = subscription.clone();
        let model_ref = model_ref.clone();
        let completion = completion.clone();
        let on_change = props.on_change.clone();
        let initial = props.value.to_string();
        let read_only = props.read_only;
        let schema = props.schema.clone();
        let validate = props.validate.clone();
        use_effect_with(node.clone(), move |node| {
            let el: HtmlElement = node.cast().expect("sql-editor node is an HtmlElement");

            define_loom_theme();

            let model =
                TextModel::create(&initial, Some("sql"), None).expect("create SQL text model");
            let opts = CodeEditorOptions::default()
                .with_language("sql".to_owned())
                .with_theme("loom-dark".to_owned())
                .with_automatic_layout(true)
                .with_model(model.clone());
            let ed = CodeEditor::create(&el, Some(opts));

            if read_only {
                let ro_opts: IEditorOptions = js_sys::Object::new().unchecked_into();
                ro_opts.set_read_only(Some(true));
                ed.as_ref().update_options_editor(&ro_opts);
            }

            // The armed server-validation debounce, owned by this mount: shared with the
            // content-change subscription and disarmed by the cleanup below.
            let pending: PendingValidation = std::rc::Rc::new(std::cell::RefCell::new(None));

            // Emit on_change with the model's current text on every edit, and
            // refresh client-side diagnostics (squiggles) from the same text.
            // Publish the model into the shared slot BEFORE anything can schedule a
            // validation: `schedule_validation` reads the model from here, and the
            // cleanup empties it so a late response knows the editor is gone.
            *model_ref.borrow_mut() = Some(model.clone());

            let cb_model = model.clone();
            let diag_schema = schema.clone();
            let cb_validate = validate.clone();
            let cb_pending = pending.clone();
            let cb_live = model_ref.clone();
            let disposable = ed.on_did_change_model_content(move |_ev| {
                on_change.emit(cb_model.get_value());
                refresh_diagnostics(&cb_model, &diag_schema);
                if let Some(v) = cb_validate.as_ref() {
                    schedule_validation(&cb_live, v, &cb_pending);
                }
            });
            *subscription.borrow_mut() = Some(disposable);
            // Seed diagnostics for the initial value (before `schema` moves into
            // the completion provider below).
            refresh_diagnostics(&model, &schema);
            // Seed server diagnostics for the initial value too. Deliberately through
            // the same debounce as a keystroke: a mount that is immediately typed into
            // then produces ONE request, not a mount request plus a typing request.
            if let Some(v) = validate.as_ref() {
                schedule_validation(&model_ref, v, &pending);
            }
            *editor.borrow_mut() = Some(ed);

            // Schema-fed completion, registered for this editor's life (see
            // `register_completion` for the one-provider-per-mount limitation).
            *completion.borrow_mut() = Some(register_completion(schema));

            move || {
                subscription.borrow_mut().take(); // unsubscribe
                // Disarm the debounce BEFORE the model is disposed: a timeout that
                // survived unmount would read (and publish markers onto) a dead model.
                cancel_pending_validation(&pending);
                editor.borrow_mut().take(); // dispose editor widget
                // Taking the model out of the shared slot is load-bearing beyond the
                // dispose: a request already in flight cannot be cancelled from here, and
                // its `respond` callback reads this slot. Emptying it first is what makes
                // that late answer a no-op instead of a `get_value()` on a disposed model.
                if let Some(model) = model_ref.borrow_mut().take() {
                    model.as_ref().dispose(); // dispose the model (editor.dispose() does not)
                }
                if let Some((_cb, reg)) = completion.borrow_mut().take() {
                    reg.dispose(); // unregister the completion provider
                    // `_cb` (the Closure) drops here, invalidating the JS callback.
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
