//! Hash-based routing for the workspace shell — the only DOM-touching piece of the
//! router. The pure model (grammar, parsing, transitions) lives in
//! `loom_ui_core::route`, where it is `rust_test`-able.
//!
//! The URL is the source of truth. [`Navigator::push`] only writes
//! `location.hash`, and the resulting `hashchange` event is what updates the state —
//! which is what makes the browser's Back/Forward buttons work with no extra
//! bookkeeping, since they fire `hashchange` exactly like a click does.
//! [`Navigator::replace`] swaps the current history entry instead; `replaceState`
//! fires no event, so it writes the state directly.

use loom_ui_core::Route;
use wasm_bindgen::JsCast;
use wasm_bindgen::JsValue;
use wasm_bindgen::closure::Closure;
use yew::prelude::*;

/// The current `window.location.hash` (`""` when there is none).
fn current_hash() -> String {
    web_sys::window()
        .and_then(|w| w.location().hash().ok())
        .unwrap_or_default()
}

/// The route the browser is on **right now**, read from the URL rather than from a
/// captured render snapshot. Async callbacks that navigate after a response lands
/// must use this: the `Route` captured when the button was built may be several
/// navigations stale by the time the server replies, and re-emitting it would yank
/// the user back to where they were.
#[must_use]
pub fn current_route() -> Route {
    Route::parse(&current_hash())
}

/// Navigation handles for the current route. See the module docs for push vs replace.
#[derive(Clone, PartialEq)]
pub struct Navigator {
    push: Callback<Route>,
    replace: Callback<Route>,
}

impl Navigator {
    /// Navigate, adding a history entry — for changes Back should undo: switching
    /// surface, opening a row.
    pub fn push(&self, next: Route) {
        self.push.emit(next);
    }

    /// Navigate, replacing the current history entry — for view adjustments (drawer
    /// tab, list sort/filter, closing the drawer after an action) that should not
    /// each cost a Back press to escape.
    pub fn replace(&self, next: Route) {
        self.replace.emit(next);
    }
}

/// The live route plus its navigator.
#[hook]
pub fn use_route() -> (Route, Navigator) {
    let route = use_state(|| Route::parse(&current_hash()));

    {
        let route = route.clone();
        use_effect_with((), move |()| {
            // Canonicalise the address bar on first load — no hash at all, or one
            // carrying redundant defaults — WITHOUT pushing a history entry, so Back
            // still leaves the app rather than bouncing between spellings.
            let canonical = route.to_hash();
            if current_hash() != canonical
                && let Some(history) = web_sys::window().and_then(|w| w.history().ok())
            {
                let _ = history.replace_state_with_url(&JsValue::NULL, "", Some(&canonical));
            }

            let on_hash_change = Closure::<dyn FnMut()>::new(move || {
                route.set(Route::parse(&current_hash()));
            });
            let window = web_sys::window();
            if let Some(w) = &window {
                let _ = w.add_event_listener_with_callback(
                    "hashchange",
                    on_hash_change.as_ref().unchecked_ref(),
                );
            }
            move || {
                if let Some(w) = window {
                    let _ = w.remove_event_listener_with_callback(
                        "hashchange",
                        on_hash_change.as_ref().unchecked_ref(),
                    );
                }
                drop(on_hash_change);
            }
        });
    }

    let push = Callback::from(|next: Route| {
        if let Some(location) = web_sys::window().map(|w| w.location()) {
            // Assigning `location.hash` pushes a history entry and fires
            // `hashchange`; assigning the SAME hash is a no-op in both respects,
            // which is the wanted behaviour for a redundant navigation.
            let _ = location.set_hash(&next.to_hash());
        }
    });

    let replace = {
        let route = route.clone();
        Callback::from(move |next: Route| {
            let hash = next.to_hash();
            if current_hash() != hash
                && let Some(history) = web_sys::window().and_then(|w| w.history().ok())
            {
                let _ = history.replace_state_with_url(&JsValue::NULL, "", Some(&hash));
            }
            // replaceState fires no `hashchange`, so the state write is ours. Compare
            // the whole Route, not the hash: `set` re-renders unconditionally, so this
            // is also what keeps a redundant navigation from costing a render.
            if *route != next {
                route.set(next);
            }
        })
    };

    ((*route).clone(), Navigator { push, replace })
}
