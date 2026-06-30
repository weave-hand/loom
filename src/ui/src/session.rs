//! Session-token persistence in the browser's sessionStorage (survives reload
//! within a tab, cleared when the tab closes). Key: `loom_token`.

const KEY: &str = "loom_token";

fn storage() -> Option<web_sys::Storage> {
    web_sys::window()?.session_storage().ok()?
}

pub fn load() -> Option<String> {
    storage()?.get_item(KEY).ok()?
}

pub fn store(token: &str) {
    if let Some(s) = storage() {
        let _ = s.set_item(KEY, token);
    }
}

pub fn clear() {
    if let Some(s) = storage() {
        let _ = s.remove_item(KEY);
    }
}
