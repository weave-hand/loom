// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Iceberg sql catalog implementation.
//!
//! Vendored from Apache iceberg-rust (`iceberg-catalog-sql`) and ported from
//! sqlx 0.8 (the `any` driver) to loom's sqlx 0.9 (PostgreSQL).

#![deny(missing_docs)]

mod catalog;
mod error;
pub mod s3_storage;
pub use catalog::*;
pub use s3_storage::S3StorageFactory;
