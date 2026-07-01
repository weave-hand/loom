use loom_ui_core::Align;
use stylist::yew::styled_component;
use yew::prelude::*;

/// A row that knows how to render itself into table cells. Callers implement this
/// for their domain struct so `DataTable` stays generic and type-safe.
pub trait TableRow {
    fn cells(&self) -> Vec<Html>;
}

#[derive(Clone, PartialEq)]
pub struct Column {
    pub label: AttrValue,
    pub align: Align,
}

#[derive(Properties, PartialEq)]
pub struct DataTableProps<R: PartialEq> {
    pub columns: Vec<Column>,
    pub rows: Vec<R>,
    #[prop_or_default]
    pub selected: Option<usize>,
    #[prop_or_default]
    pub onrow: Callback<usize>,
}

#[styled_component(DataTable)]
pub fn data_table<R>(props: &DataTableProps<R>) -> Html
where
    R: PartialEq + Clone + TableRow + 'static,
{
    let cls = css!(
        r#"
        width: 100%; border-collapse: collapse; font-size: 13px;
        th, td { padding: 6px 10px; border-bottom: 1px solid var(--loom-border); }
        th { color: var(--loom-text-mut); font-weight: 500; text-align: left; font-size: 12px; }
        tbody tr { cursor: pointer; }
        tbody tr:hover { background: var(--loom-panel-2); }
        tbody tr.selected { background: color-mix(in srgb, var(--loom-accent) 18%, transparent); }
        td.end { text-align: right; font-variant-numeric: tabular-nums; }
    "#
    );
    html! {
        <table class={cls}>
            <thead>
                <tr>
                    { for props.columns.iter().map(|c| {
                        let end = matches!(c.align, Align::End);
                        html! { <th class={classes!(end.then_some("end"))}>{ &c.label }</th> }
                    }) }
                </tr>
            </thead>
            <tbody>
                { for props.rows.iter().enumerate().map(|(i, row)| {
                    let selected = props.selected == Some(i);
                    let onrow = props.onrow.clone();
                    let onclick = Callback::from(move |_| onrow.emit(i));
                    let cells = row.cells();
                    html! {
                        <tr class={classes!(selected.then_some("selected"))} {onclick}>
                            { for props.columns.iter().zip(cells).map(|(c, cell)| {
                                let end = matches!(c.align, Align::End);
                                html! { <td class={classes!(end.then_some("end"))}>{ cell }</td> }
                            }) }
                        </tr>
                    }
                }) }
            </tbody>
        </table>
    }
}
