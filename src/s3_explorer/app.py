import dash_ag_grid as dag
import pyarrow as pa
from dash import (
    Dash,
    Input,
    Output,
    State,
    callback,
    dcc,
    html,
    no_update,
)
from obstore.store import S3Store

# Global state for storing config and iterators
_app_state = {}

# Fixed column definitions for S3 object listing
COLUMN_DEFS = [
    {"field": "path", "headerName": "Path", "flex": 2},
    {"field": "size", "headerName": "Size", "flex": 1},
    {"field": "last_modified", "headerName": "Last Modified", "flex": 1},
]


def create_app(cfg, logger):
    app = Dash(__name__, suppress_callback_exceptions=True)

    bucket_names = list(cfg.buckets.keys())
    _app_state["cfg"] = cfg
    _app_state["logger"] = logger
    _app_state["exhausted"] = False
    _app_state["all_data"] = []

    app.layout = html.Div(
        [
            dcc.Dropdown(
                id="bucket-select",
                options=[{"label": name, "value": name} for name in bucket_names],
                placeholder="Select a bucket",
            ),
            dcc.Store(id="current-bucket", data=None),
            dag.AgGrid(
                id="objects-table",
                columnDefs=COLUMN_DEFS,
                defaultColDef={"filter": True, "sortable": True, "resizable": True},
                style={"height": "500px", "width": "100%"},
                rowModelType="infinite",
                dashGridOptions={
                    "rowBuffer": 0,
                    "cacheBlockSize": 100,
                    "cacheOverflowSize": 2,
                    "maxConcurrentDatasourceRequests": 1,
                    "infiniteInitialRowCount": 100,
                    "maxBlocksInCache": 10,
                },
            ),
        ]
    )

    @callback(
        Output("current-bucket", "data"),
        Input("bucket-select", "value"),
        State("current-bucket", "data"),
        prevent_initial_call=True,
    )
    def on_bucket_change(selected_bucket, current_bucket):
        if not selected_bucket:
            return None

        cfg = _app_state["cfg"]

        # Reset state
        _app_state["exhausted"] = False
        _app_state["all_data"] = []
        _app_state.pop("stream", None)

        bucket_cfg = cfg.buckets[selected_bucket]

        config = {
            "aws_endpoint": bucket_cfg.endpoint,
        }

        if bucket_cfg.get("access_key"):
            config["aws_access_key_id"] = bucket_cfg.access_key
            config["aws_secret_access_key"] = bucket_cfg.secret_key
        else:
            config["aws_skip_signature"] = True

        store = S3Store(
            bucket=selected_bucket,
            config=config,
        )
        _app_state["stream"] = store.list(return_arrow=True)

        return selected_bucket

    @callback(
        Output("objects-table", "getRowsResponse"),
        Input("objects-table", "getRowsRequest"),
        Input("current-bucket", "data"),
        prevent_initial_call=True,
    )
    def get_rows(request, current_bucket):
        if not current_bucket or "stream" not in _app_state:
            return no_update

        # If no request yet, return initial empty response to trigger a request
        if not request:
            return {"rowData": [], "rowCount": -1}

        start_row = request["startRow"]
        end_row = request["endRow"]

        # Load more data if needed
        while len(_app_state["all_data"]) < end_row and not _app_state.get(
            "exhausted", False
        ):
            try:
                chunk = next(_app_state["stream"])
                df = pa.record_batch(chunk).to_pandas()
                # Convert datetime columns
                for col in df.columns:
                    if "datetime" in str(df[col].dtype):
                        df[col] = df[col].astype(str)
                new_rows = df.to_dict("records")
                _app_state["all_data"].extend(new_rows)
            except StopIteration:
                _app_state["exhausted"] = True
                break

        rows = _app_state["all_data"][start_row:end_row]

        if _app_state.get("exhausted", False):
            row_count = len(_app_state["all_data"])
        else:
            row_count = -1

        return {"rowData": rows, "rowCount": row_count}

    return app
