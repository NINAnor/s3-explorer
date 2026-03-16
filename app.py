import pathlib

import humanize
import ibis
import pyarrow as pa
import pyarrow.parquet as pq
import streamlit as st
from obstore.store import S3Store
from omegaconf import OmegaConf


def load_config():
    """Load configuration from config.yaml."""
    config_path = pathlib.Path.cwd() / "config.yaml"
    if config_path.exists():
        return OmegaConf.load(config_path)
    return OmegaConf.create({"buckets": {}})


def create_store(
    bucket_name: str, endpoint: str, access_key: str | None, secret_key: str | None
) -> S3Store:
    """Create an S3Store for the given bucket."""
    config = {
        "aws_endpoint": endpoint,
    }

    if access_key:
        config["aws_access_key_id"] = access_key
        config["aws_secret_access_key"] = secret_key
    else:
        config["aws_skip_signature"] = True

    return S3Store(
        bucket=bucket_name,
        config=config,
    )


@st.cache_data
def load_bucket_contents(
    bucket_name: str, endpoint: str, access_key: str | None, secret_key: str | None
) -> pa.Table | None:
    """Load all contents from a bucket and return as Arrow table."""
    store = create_store(bucket_name, endpoint, access_key, secret_key)

    batches = []
    for chunk in store.list(return_arrow=True):
        batches.append(pa.record_batch(chunk))

    if batches:
        table = ibis.memtable(pa.Table.from_batches(batches))

        result = table.select("path", "size", "last_modified").to_pyarrow()

        # Convert size to human readable
        size_col = result.column("size")
        human_sizes = [
            humanize.naturalsize(s.as_py()) if s.as_py() is not None else ""
            for s in size_col
        ]

        return result.set_column(
            result.schema.get_field_index("size"),
            "size",
            pa.array(human_sizes),
        )
    return None


@st.cache_resource
def load_file_content(
    bucket_name: str,
    endpoint: str,
    access_key: str | None,
    secret_key: str | None,
    path: str,
) -> bytes:
    """Load file content from S3."""
    store = create_store(bucket_name, endpoint, access_key, secret_key)
    return bytes(store.get(path).bytes())


def preview_file(bucket_cfg, bucket_name: str, path: str):
    """Preview file content based on extension."""
    ext = pathlib.Path(path).suffix.lower()

    try:
        content = load_file_content(
            bucket_name,
            bucket_cfg.endpoint,
            bucket_cfg.get("access_key"),
            bucket_cfg.get("secret_key"),
            path,
        )

        if ext in (".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg"):
            st.image(content, caption=path)
        elif ext == ".parquet":
            table = pq.read_table(pa.BufferReader(content))
            st.info(f"{table.num_rows} rows, {table.num_columns} columns")
            st.dataframe(table, width="stretch", height=400)
        elif ext in (".txt", ".md", ".json", ".yaml", ".yml", ".csv", ".xml"):
            st.code(content.decode("utf-8"), language=ext.lstrip("."))
        else:
            st.warning(f"No preview available for {ext} files")
            st.download_button(
                "Download file", content, file_name=pathlib.Path(path).name
            )
    except Exception as e:
        st.error(f"Error loading file: {e}")


def run_app():
    """Run the Streamlit app."""
    st.set_page_config(page_title="S3 Explorer", layout="wide")
    st.sidebar.title("S3 Explorer")

    cfg = load_config()
    bucket_names = list(cfg.buckets.keys()) if cfg.buckets else []

    if not bucket_names:
        st.warning("No buckets configured. Add buckets to config.yaml")
        return

    # Bucket selector in sidebar
    selected_bucket = st.sidebar.selectbox(
        "Select a bucket",
        options=[""] + bucket_names,
        index=0,
    )

    if selected_bucket:
        bucket_cfg = cfg.buckets[selected_bucket]

        with st.spinner("Loading bucket contents..."):
            table = load_bucket_contents(
                selected_bucket,
                bucket_cfg.endpoint,
                bucket_cfg.get("access_key"),
                bucket_cfg.get("secret_key"),
            )

            if table:
                st.info(f"{table.num_rows} items found")

                event = st.dataframe(
                    table,
                    width="stretch",
                    height=500,
                    selection_mode="single-row",
                    on_select="rerun",
                )

                # Show preview if a row is selected
                if event.selection and event.selection.rows:
                    selected_idx = event.selection.rows[0]
                    selected_path = table.column("path")[selected_idx].as_py()

                    st.subheader(
                        f"Preview: {bucket_cfg.endpoint}/{selected_bucket}/{selected_path}"
                    )
                    preview_file(bucket_cfg, selected_bucket, selected_path)
            else:
                st.info("Bucket is empty")


if __name__ == "__main__":
    run_app()
