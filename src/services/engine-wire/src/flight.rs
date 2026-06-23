//! Arrow Flight wire types shared between the engine and its clients.
//!
//! Today this holds [`FlightTicket`], the ticket payload naming the data files
//! a Flight `do_get` will stream. The zero-pool table-stream client lands in a
//! later task.

use serde::{Deserialize, Serialize};

/// What a Flight `Ticket` names: an explicit set of a table's data files to
/// stream. `files` are the data-file path strings exactly as stored in the
/// iceberg mirror (passed verbatim to the engine's `FileIO::new_input`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlightTicket {
    pub schema: String,
    pub name: String,
    pub files: Vec<String>,
}

impl FlightTicket {
    /// JSON-encode for the `Ticket.ticket` bytes.
    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("FlightTicket is always serializable")
    }

    /// Decode from `Ticket.ticket` bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(bytes)
    }
}
