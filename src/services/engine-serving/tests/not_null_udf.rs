//! `loom_not_null`: the identity scalar UDF that re-declares its argument's
//! field as NON-nullable. Used by `build_merge_view`'s final projection to
//! restore the mirror's declared nullability above the `_loom_tomb = false`
//! filter, so the merged view's served schema stays byte-identical to the
//! mirror (iss-search-vector-merge-view-nullable — the transform/MV blast
//! radius the inline-tier widening would otherwise cause).

use std::sync::Arc;

use arrow::array::{Float32Builder, Int64Array, ListBuilder, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::prelude::SessionContext;

/// A projection through `loom_not_null` over a NULLABLE input column yields a
/// NON-nullable output field, with values untouched — logically (the DataFrame
/// schema) and physically (the collected batch's schema, i.e. `ProjectionExec`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn declares_non_nullable_and_passes_values_through() {
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1_i64, 2, 3]))])
        .expect("batch");
    let ctx = SessionContext::new();
    let df = ctx.read_batch(batch).expect("read_batch");

    let out = df
        .select(vec![
            engine_serving::not_null::not_null(datafusion::prelude::col("v")).alias("v"),
        ])
        .expect("select");

    assert!(
        !out.schema()
            .field_with_unqualified_name("v")
            .expect("v")
            .is_nullable(),
        "loom_not_null re-declares the field NON-nullable at the logical level"
    );
    let batches = out.collect().await.expect("collect");
    let got: Vec<i64> = batches
        .iter()
        .flat_map(|b| {
            let a = b
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("Int64");
            (0..b.num_rows()).map(|i| a.value(i)).collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(got, vec![1, 2, 3], "values pass through unchanged");
    assert!(
        !batches
            .first()
            .expect("one batch")
            .schema()
            .field(0)
            .is_nullable(),
        "and NON-nullable at the physical/batch level (ProjectionExec)"
    );
}

/// The signature must coerce over a `List<Float32>` — a `vector(N)` column, the
/// exact type the defect was found on. `Signature::any` is the plan's first
/// choice; if DataFusion 54 refuses to coerce a List under it, this case is what
/// forces the `Signature::user_defined` + identity `coerce_types` fallback.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn coerces_over_a_list_float32_vector_column() {
    let item = Arc::new(Field::new("item", DataType::Float32, false));
    let mut lb = ListBuilder::new(Float32Builder::new()).with_field(item.clone());
    lb.values().append_slice(&[1.0_f32, 0.0, 0.0, 0.0]);
    lb.append(true);
    let schema = Arc::new(Schema::new(vec![Field::new(
        "embedding",
        DataType::List(item),
        true,
    )]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(lb.finish())]).expect("batch");
    let ctx = SessionContext::new();
    let out = ctx
        .read_batch(batch)
        .expect("read_batch")
        .select(vec![
            engine_serving::not_null::not_null(datafusion::prelude::col("embedding"))
                .alias("embedding"),
        ])
        .expect("select");

    let field = out
        .schema()
        .field_with_unqualified_name("embedding")
        .expect("embedding")
        .clone();
    assert!(
        !field.is_nullable(),
        "a List<Float32> column is re-declared NON-nullable too"
    );
    assert!(
        matches!(field.data_type(), DataType::List(_)),
        "the data type is passed through verbatim, got {:?}",
        field.data_type()
    );
    let batches = out.collect().await.expect("collect");
    let first = batches.first().expect("one batch");
    assert_eq!(first.num_rows(), 1, "the row passes through");
    assert!(
        !first.schema().field(0).is_nullable(),
        "and NON-nullable physically over a List column"
    );
}
