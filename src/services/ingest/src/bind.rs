//! dataset->model binding: validate a landed table's physical schema against a
//! declared ontology type, then persist it (define_type). A bound type is
//! guaranteed-serveable by the query read path. See the part-2b design doc.

use control_plane_core::{
    BaseType, Catalog, ControlPlaneError, DerivedPropertyDef, LinkBacking, LinkDef, ObjectType,
    Ontology, PageReq, TableRef, TableSchema, TypeName, UnknownLogicalType, resolve_logical,
    satisfies,
};

#[derive(Debug, thiserror::Error)]
pub enum BindError {
    #[error("table not found in catalog: {0:?}")]
    TableNotFound(TableRef),
    #[error("type does not conform to the landed table: {} violation(s)", .0.len())]
    DoesNotConform(Vec<BindViolation>),
    #[error(transparent)]
    ControlPlane(#[from] ControlPlaneError),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BindViolation {
    pub property: String,
    pub reason: BindViolationReason,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BindViolationReason {
    MissingColumn,
    UnknownLogicalType(String),
    TypeMismatch {
        logical: String,
        physical: String,
    },
    NullabilityViolation, // a required property backed by a nullable column
    BadIdentity(String),  // identity names no declared property, or a non-required one
    ReservedName,         // a property/derived name begins with `_`, reserved for control params
    /// A derived property names a link not defined (outbound) on this type.
    UnknownDerivedLink(String),
    /// A derived aggregation's column is absent from the link target's table.
    MissingAggColumn,
    /// An aggregation is not applicable to its column's logical type
    /// (Sum/Avg need numeric; Min/Max need an ordered type).
    BadAggType {
        agg: String,
        column: String,
    },
    /// A derived property's declared result type is unknown or inconsistent with
    /// the aggregation's result category.
    BadDerivedResultType {
        declared: String,
        expected: String,
    },
}

/// Validate `type_def` against the physical schema of its target table, then persist
/// it via `define_type`. Collects ALL violations; persists nothing on rejection.
pub async fn bind(
    catalog: &dyn Catalog,
    ontology: &dyn Ontology,
    type_def: ObjectType,
) -> Result<(), BindError> {
    // 1. The table must be live in the catalog.
    let snap = match catalog.current_snapshot(&type_def.table).await {
        Ok(s) => s,
        Err(ControlPlaneError::NotFound(_)) => {
            return Err(BindError::TableNotFound(type_def.table.clone()));
        }
        Err(e) => return Err(BindError::ControlPlane(e)),
    };

    // 2. Its physical columns at that snapshot.
    let schema = catalog.schema(&type_def.table, snap.id).await?;

    // 3. Validate every declared property against its same-named physical column.
    //    Extra physical columns are fine — a type is a view over the table.
    // The type check and the nullability check are INDEPENDENT: a single property
    // may yield up to two violations (e.g. a type mismatch AND a required-but-nullable
    // column). We report both so the caller fixes everything in one pass rather than
    // discovering problems one round-trip at a time. (A MissingColumn short-circuits —
    // there's nothing to type/nullability-check.)
    let mut violations = Vec::new();
    for p in &type_def.properties {
        let Some(col) = schema.columns.iter().find(|c| c.name == p.name) else {
            violations.push(BindViolation {
                property: p.name.clone(),
                reason: BindViolationReason::MissingColumn,
            });
            continue;
        };
        match satisfies(&p.ty, &col.ty) {
            Err(UnknownLogicalType(t)) => violations.push(BindViolation {
                property: p.name.clone(),
                reason: BindViolationReason::UnknownLogicalType(t),
            }),
            Ok(false) => violations.push(BindViolation {
                property: p.name.clone(),
                reason: BindViolationReason::TypeMismatch {
                    logical: p.ty.clone(),
                    physical: col.ty.clone(),
                },
            }),
            Ok(true) => {}
        }
        if p.required && col.nullable {
            violations.push(BindViolation {
                property: p.name.clone(),
                reason: BindViolationReason::NullabilityViolation,
            });
        }
    }

    // Identity (if declared) must name a declared, required property — a primary key
    // cannot be nullable. The violation's `property` is the named identity column.
    if let Some(id) = &type_def.identity {
        match type_def.properties.iter().find(|p| &p.name == id) {
            None => violations.push(BindViolation {
                property: id.clone(),
                reason: BindViolationReason::BadIdentity("names no declared property".into()),
            }),
            Some(p) if !p.required => violations.push(BindViolation {
                property: id.clone(),
                reason: BindViolationReason::BadIdentity("names a non-required property".into()),
            }),
            Some(_) => {}
        }
    }

    // Property and derived-property names beginning with `_` are reserved: the query
    // surface prefixes control params with `_` (e.g. `_ids`, `_path`), so a `_`-named
    // property would be unaddressable as a filter and could shadow a control param.
    for p in &type_def.properties {
        if p.name.starts_with('_') {
            violations.push(BindViolation {
                property: p.name.clone(),
                reason: BindViolationReason::ReservedName,
            });
        }
    }
    for d in &type_def.derived {
        if d.name.starts_with('_') {
            violations.push(BindViolation {
                property: d.name.clone(),
                reason: BindViolationReason::ReservedName,
            });
        }
    }

    // Derived properties: validate each against the ontology + the link target's
    // physical schema. This front-runs the read-time omission in query-api's
    // handler (a missing link / target / agg column there silently drops the
    // property — see `handler.rs`). The link must be defined on THIS type; if the
    // type is not yet defined it has no links (`NotFound` -> empty), so every
    // derived link is unknown, encoding the authoring order (types -> links ->
    // bind-with-derived).
    if !type_def.derived.is_empty() {
        let links = match ontology.links(&type_def.name, PageReq::unbounded()).await {
            Ok(p) => p.items,
            Err(ControlPlaneError::NotFound(_)) => Vec::new(),
            Err(e) => return Err(BindError::ControlPlane(e)),
        };
        for d in &type_def.derived {
            validate_derived(catalog, ontology, &links, d, &mut violations).await?;
        }
    }

    if !violations.is_empty() {
        return Err(BindError::DoesNotConform(violations));
    }

    // 4. Persist — the type is now serveable by the governed read path.
    ontology.define_type(type_def).await?;
    Ok(())
}

/// The result of resolving an aggregation's target column. `Missing` is the three
/// absent cases (target type, its live snapshot, or the column) the caller reports as
/// a single [`BindViolationReason::MissingAggColumn`]; `Present` carries the column's
/// resolved base type (`None` if its logical type is unknown — a present column, so
/// NOT a `MissingAggColumn`).
enum ColumnLookup {
    Missing,
    Present(Option<BaseType>),
}

/// Resolve `col_name` on `link`'s target type's table into a [`ColumnLookup`]. Any
/// non-`NotFound` control-plane error propagates.
async fn target_column_type(
    catalog: &dyn Catalog,
    ontology: &dyn Ontology,
    link: &LinkDef,
    col_name: &str,
) -> Result<ColumnLookup, BindError> {
    let target_table = match ontology.resolve(&link.to).await {
        Ok(t) => t,
        Err(ControlPlaneError::NotFound(_)) => return Ok(ColumnLookup::Missing),
        Err(e) => return Err(BindError::ControlPlane(e)),
    };
    let snap = match catalog.current_snapshot(&target_table).await {
        Ok(s) => s,
        Err(ControlPlaneError::NotFound(_)) => return Ok(ColumnLookup::Missing),
        Err(e) => return Err(BindError::ControlPlane(e)),
    };
    let schema = catalog.schema(&target_table, snap.id).await?;
    match schema.columns.iter().find(|c| c.name == col_name) {
        None => Ok(ColumnLookup::Missing),
        Some(col) => Ok(ColumnLookup::Present(resolve_logical(&col.ty))),
    }
}

/// Validate one derived property against the type's outbound `links` and the link
/// target's physical schema, pushing any [`BindViolation`]s. Mirrors the read-time
/// resolution in query-api's handler so a reference that would be silently dropped
/// at read time is instead rejected at authoring time.
async fn validate_derived(
    catalog: &dyn Catalog,
    ontology: &dyn Ontology,
    links: &[LinkDef],
    d: &DerivedPropertyDef,
    violations: &mut Vec<BindViolation>,
) -> Result<(), BindError> {
    // 1. The named link must be defined (outbound) on this type.
    let Some(link) = links.iter().find(|l| l.name == d.link) else {
        violations.push(BindViolation {
            property: d.name.clone(),
            reason: BindViolationReason::UnknownDerivedLink(d.link.clone()),
        });
        return Ok(());
    };

    // 2. For column-bearing aggregations, resolve the target column and check the
    //    aggregation is applicable to its logical type. `Count` takes no column.
    let mut col_base: Option<BaseType> = None;
    if let Some(col_name) = d.agg.column() {
        match target_column_type(catalog, ontology, link, col_name).await? {
            ColumnLookup::Missing => {
                violations.push(BindViolation {
                    property: d.name.clone(),
                    reason: BindViolationReason::MissingAggColumn,
                });
                return Ok(());
            }
            ColumnLookup::Present(base) => {
                col_base = base;
                if !d.agg.column_applicable(col_base) {
                    // Collect-all: keep going to also report a result-type mismatch.
                    violations.push(BindViolation {
                        property: d.name.clone(),
                        reason: BindViolationReason::BadAggType {
                            agg: d.agg.label().to_string(),
                            column: col_name.to_string(),
                        },
                    });
                }
            }
        }
    }

    // 3. The declared result type must be a known logical type and consistent with the
    //    aggregation's result category. (Existence + category only; the full coercion
    //    lattice is deferred — see fut-coercion-taxonomy.)
    let declared = resolve_logical(&d.ty);
    let category = d.agg.result_expectation(col_base);
    if !category.accepts(declared) {
        violations.push(BindViolation {
            property: d.name.clone(),
            reason: BindViolationReason::BadDerivedResultType {
                declared: d.ty.clone(),
                expected: category.description(),
            },
        });
    }
    Ok(())
}

/// Validate a link's physical backing columns against the catalog, then persist it
/// via [`Ontology::define_link`] — the define-time gate for `define_link`, mirroring
/// [`bind`] for `define_type`. Endpoint *types* are already checked by `define_link`
/// itself; `bind_link` adds the physical-column gate. Collects ALL violations;
/// persists nothing on rejection.
pub async fn bind_link(
    catalog: &dyn Catalog,
    ontology: &dyn Ontology,
    link: LinkDef,
) -> Result<(), BindError> {
    let mut violations = Vec::new();
    match &link.backing {
        // Direct equijoin `from_table.from_column = to_table.to_column`.
        LinkBacking::ForeignKey {
            from_column,
            to_column,
        } => {
            let from_schema = schema_of_type(catalog, ontology, &link.from).await?;
            let to_schema = schema_of_type(catalog, ontology, &link.to).await?;
            check_column(&from_schema, from_column, &mut violations);
            check_column(&to_schema, to_column, &mut violations);
        }
        // Many-to-many through a mapping table:
        //   `from_table.from_key = table.from_column AND table.to_column = to_table.to_key`.
        // NOTE the column->table assignment (verified against query-api's read-time
        // SQL and the LinkBacking::JoinTable doc comment): `from_key` is on the
        // FROM type's table, `from_column`/`to_column` on the JOIN table, and
        // `to_key` on the TO type's table. (The design spec's prose had this
        // backwards; the read path is authoritative.)
        LinkBacking::JoinTable {
            table,
            from_key,
            from_column,
            to_column,
            to_key,
        } => {
            let from_schema = schema_of_type(catalog, ontology, &link.from).await?;
            let to_schema = schema_of_type(catalog, ontology, &link.to).await?;
            // A join table absent from the catalog is a hard TableNotFound, matching
            // `bind`'s own table-not-found (vs. a missing endpoint TYPE, which is
            // `define_link`'s precondition and surfaces as ControlPlane(NotFound)).
            let join_schema = schema_of_table(catalog, table).await?;
            check_column(&from_schema, from_key, &mut violations);
            check_column(&join_schema, from_column, &mut violations);
            check_column(&join_schema, to_column, &mut violations);
            check_column(&to_schema, to_key, &mut violations);
        }
    }

    if !violations.is_empty() {
        return Err(BindError::DoesNotConform(violations));
    }

    ontology.define_link(link).await?;
    Ok(())
}

/// Push a [`BindViolationReason::MissingColumn`] (whose `property` names the column)
/// if `column` is absent from `schema`.
fn check_column(schema: &TableSchema, column: &str, violations: &mut Vec<BindViolation>) {
    if !schema.columns.iter().any(|c| c.name == column) {
        violations.push(BindViolation {
            property: column.to_string(),
            reason: BindViolationReason::MissingColumn,
        });
    }
}

/// The current physical schema of `table`. A table not live in the catalog is a
/// [`BindError::TableNotFound`], matching `bind`.
async fn schema_of_table(
    catalog: &dyn Catalog,
    table: &TableRef,
) -> Result<TableSchema, BindError> {
    let snap = match catalog.current_snapshot(table).await {
        Ok(s) => s,
        Err(ControlPlaneError::NotFound(_)) => return Err(BindError::TableNotFound(table.clone())),
        Err(e) => return Err(BindError::ControlPlane(e)),
    };
    Ok(catalog.schema(table, snap.id).await?)
}

/// The current physical schema of the table backing ontology type `ty`. A missing
/// endpoint type surfaces as `ControlPlane(NotFound)` (endpoint types are
/// `define_link`'s precondition — see `bind_link`), distinct from a missing backing
/// table, which is `TableNotFound`.
async fn schema_of_type(
    catalog: &dyn Catalog,
    ontology: &dyn Ontology,
    ty: &TypeName,
) -> Result<TableSchema, BindError> {
    let table = ontology.resolve(ty).await?;
    schema_of_table(catalog, &table).await
}
