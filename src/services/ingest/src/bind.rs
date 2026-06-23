//! dataset->model binding: validate a landed DuckLake table's physical schema
//! against a declared ontology type, then persist it (define_type). A bound type is
//! guaranteed-serveable by the query read path. See the part-2b design doc.

use control_plane_core::{
    Aggregation, BaseType, Catalog, ControlPlaneError, LinkBacking, LinkDef, ObjectType, Ontology,
    PageReq, TableRef, TableSchema, UnknownLogicalType, resolve_logical, satisfies,
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
    /// A derived property names a link not defined on this type.
    UnknownDerivedLink(String),
    /// A derived property's aggregation column is absent from the target table.
    MissingAggColumn,
    /// The aggregation is not applicable to the target column's type (e.g. Sum over a
    /// non-numeric column, Min/Max over an unordered column). `agg` names the
    /// aggregation, `column` the offending target column.
    BadAggType {
        agg: String,
        column: String,
    },
    /// The derived property's declared result type is unknown, or inconsistent with the
    /// aggregation's result category (Count -> integer, Sum/Avg -> numeric, Min/Max ->
    /// the column's type). `declared` is the declared logical type, `expected` the
    /// required category/type.
    BadDerivedResultType {
        declared: String,
        expected: String,
    },
}

/// Sum/Avg require a numeric column.
fn is_numeric(b: BaseType) -> bool {
    matches!(b, BaseType::Integer | BaseType::Long | BaseType::Double)
}

/// Min/Max require an ordered column. Boolean is excluded (a degenerate min/max).
fn is_ordered(b: BaseType) -> bool {
    matches!(
        b,
        BaseType::Integer
            | BaseType::Long
            | BaseType::Double
            | BaseType::Date
            | BaseType::Timestamp
            | BaseType::String
    )
}

/// Human name of an aggregation, for the `BadAggType` violation.
fn agg_name(a: &Aggregation) -> &'static str {
    match a {
        Aggregation::Count => "Count",
        Aggregation::Sum(_) => "Sum",
        Aggregation::Avg(_) => "Avg",
        Aggregation::Min(_) => "Min",
        Aggregation::Max(_) => "Max",
    }
}

/// The column an aggregation reads, if any (Count reads none).
fn agg_column(a: &Aggregation) -> Option<&str> {
    match a {
        Aggregation::Count => None,
        Aggregation::Sum(c) | Aggregation::Avg(c) | Aggregation::Min(c) | Aggregation::Max(c) => {
            Some(c.as_str())
        }
    }
}

/// `table`'s schema at its current snapshot, mapping a not-live table to
/// `BindError::TableNotFound` (the same shape `bind` uses for its primary table).
async fn schema_of(catalog: &dyn Catalog, table: &TableRef) -> Result<TableSchema, BindError> {
    let snap = match catalog.current_snapshot(table).await {
        Ok(s) => s,
        Err(ControlPlaneError::NotFound(_)) => return Err(BindError::TableNotFound(table.clone())),
        Err(e) => return Err(BindError::ControlPlane(e)),
    };
    Ok(catalog.schema(table, snap.id).await?)
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

    // Derived-property validation: each derived property must name a link defined on
    // this type, and its aggregation must be applicable to the target column with a
    // consistent result type. Collected alongside the property violations above.
    if !type_def.derived.is_empty() {
        let links = match ontology.links(&type_def.name, PageReq::unbounded()).await {
            Ok(page) => page.items,
            // The type being bound is not persisted until the end of bind, so a
            // brand-new type has no links yet -> every derived link is unknown. This
            // enforces the authoring order: define types -> define links -> bind with
            // derived properties.
            Err(ControlPlaneError::NotFound(_)) => Vec::new(),
            Err(e) => return Err(BindError::ControlPlane(e)),
        };
        for d in &type_def.derived {
            let Some(link) = links.iter().find(|l| l.name == d.link) else {
                violations.push(BindViolation {
                    property: d.name.clone(),
                    reason: BindViolationReason::UnknownDerivedLink(d.link.clone()),
                });
                continue;
            };
            // The column's logical type from the target table (None for Count, or when
            // the column is absent). Only column-bearing aggregations read the target
            // schema — Count needs no target read.
            let col_base: Option<BaseType> = if let Some(col_name) = agg_column(&d.agg) {
                let target_table = ontology.resolve(&link.to).await?;
                let schema = schema_of(catalog, &target_table).await?;
                match schema.columns.iter().find(|c| c.name == col_name) {
                    None => {
                        violations.push(BindViolation {
                            property: d.name.clone(),
                            reason: BindViolationReason::MissingAggColumn,
                        });
                        None // column missing -> applicability + Min/Max result checks skipped
                    }
                    Some(col) => {
                        let cb = resolve_logical(&col.ty);
                        // Applicability: Sum/Avg numeric, Min/Max ordered.
                        let applicable = match (&d.agg, cb) {
                            (Aggregation::Sum(_) | Aggregation::Avg(_), Some(b)) => is_numeric(b),
                            (Aggregation::Min(_) | Aggregation::Max(_), Some(b)) => is_ordered(b),
                            // An unrecognized column type cannot satisfy any aggregation.
                            (_, None) => false,
                            _ => true,
                        };
                        if !applicable {
                            violations.push(BindViolation {
                                property: d.name.clone(),
                                reason: BindViolationReason::BadAggType {
                                    agg: agg_name(&d.agg).into(),
                                    column: col_name.into(),
                                },
                            });
                        }
                        cb
                    }
                }
            } else {
                None
            };
            // Result-type consistency: the declared `ty` must be a known logical type
            // and match the aggregation's result category.
            let declared = resolve_logical(&d.ty);
            let (ok, expected): (bool, String) = match &d.agg {
                Aggregation::Count => (
                    matches!(declared, Some(BaseType::Integer | BaseType::Long)),
                    "integer".into(),
                ),
                Aggregation::Sum(_) | Aggregation::Avg(_) => {
                    (declared.is_some_and(is_numeric), "numeric".into())
                }
                Aggregation::Min(_) | Aggregation::Max(_) => match col_base {
                    // Min/Max preserve the column's type; if the column was absent we
                    // already reported MissingAggColumn and skip this check.
                    Some(cb) => (declared == Some(cb), cb.canonical_name().into()),
                    None => (true, String::new()),
                },
            };
            if !ok {
                violations.push(BindViolation {
                    property: d.name.clone(),
                    reason: BindViolationReason::BadDerivedResultType {
                        declared: d.ty.clone(),
                        expected,
                    },
                });
            }
        }
    }

    if !violations.is_empty() {
        return Err(BindError::DoesNotConform(violations));
    }

    // 4. Persist — the type is now serveable by the governed read path.
    ontology.define_type(type_def).await?;
    Ok(())
}

/// Validate a link's backing columns against the catalog, then persist it via
/// `define_link` only when clean. `define_link` already checks both endpoint *types*
/// exist; `bind_link` adds the physical-column gate, collecting ALL missing-column
/// violations (a missing column is a `MissingColumn` whose `property` names the column).
///
/// The column→table mapping follows the runtime join the traversal compiler emits
/// (`query-api::sql`, and the [`LinkBacking`] doc-comment), NOT a naive reading of
/// field names:
/// - **ForeignKey**: `from_column` on the from-type's table, `to_column` on the to-type's.
/// - **JoinTable**: `from_key` on the from-type's table, `to_key` on the to-type's table,
///   and `from_column` + `to_column` on the join table (which must be live, else
///   `BindError::TableNotFound`).
pub async fn bind_link(
    catalog: &dyn Catalog,
    ontology: &dyn Ontology,
    link: LinkDef,
) -> Result<(), BindError> {
    // Endpoint tables. A missing endpoint type surfaces as ControlPlane(NotFound) from
    // resolve (define_link would reject it too); a non-catalogued endpoint table is a
    // TableNotFound, the same shape `bind` uses.
    let from_table = ontology.resolve(&link.from).await?;
    let to_table = ontology.resolve(&link.to).await?;
    let from_schema = schema_of(catalog, &from_table).await?;
    let to_schema = schema_of(catalog, &to_table).await?;

    let mut violations = Vec::new();
    let require = |schema: &TableSchema, col: &str, out: &mut Vec<BindViolation>| {
        if !schema.columns.iter().any(|c| c.name == col) {
            out.push(BindViolation {
                property: col.to_string(),
                reason: BindViolationReason::MissingColumn,
            });
        }
    };
    match &link.backing {
        LinkBacking::ForeignKey {
            from_column,
            to_column,
        } => {
            require(&from_schema, from_column, &mut violations);
            require(&to_schema, to_column, &mut violations);
        }
        LinkBacking::JoinTable {
            table,
            from_key,
            from_column,
            to_column,
            to_key,
        } => {
            let join_schema = schema_of(catalog, table).await?;
            require(&from_schema, from_key, &mut violations);
            require(&to_schema, to_key, &mut violations);
            require(&join_schema, from_column, &mut violations);
            require(&join_schema, to_column, &mut violations);
        }
    }
    if !violations.is_empty() {
        return Err(BindError::DoesNotConform(violations));
    }

    ontology.define_link(link).await?;
    Ok(())
}
