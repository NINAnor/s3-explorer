import os
import pathlib

import geoarrow.pyarrow as ga
import humanize
import ibis
import leafmap.foliumap as leafmap
import obstore.fsspec
import pyarrow as pa
import pyarrow.parquet as pq
import streamlit as st
from geoarrow.pyarrow.io import read_geoparquet_table
from obstore.store import S3Store
from omegaconf import OmegaConf
from pyogrio import read_info


def load_config():
    """Load config from config.yaml or S3_EXPLORER_CONFIG env var."""
    config_path = pathlib.Path(
        os.environ.get("S3_EXPLORER_CONFIG", pathlib.Path.cwd() / "config.yaml")
    )
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


def create_fs(
    bucket_name: str, endpoint: str, access_key: str | None, secret_key: str | None
) -> obstore.fsspec.FsspecStore:
    """Create an fsspec filesystem for the given bucket."""
    config = {
        "endpoint": endpoint,
    }
    if access_key:
        config["access_key_id"] = access_key
        config["secret_access_key"] = secret_key
    else:
        config["skip_signature"] = True
    return obstore.fsspec.FsspecStore("s3", **config)


@st.cache_data()
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

    http_path = f"{bucket_cfg.endpoint}/{bucket_cfg.bucket}/{path}"
    fs = create_fs(
        bucket_cfg.bucket or bucket_name,
        bucket_cfg.endpoint,
        bucket_cfg.get("access_key"),
        bucket_cfg.get("secret_key"),
    )

    s3_path = f"s3://{bucket_name}/{path}"

    try:
        if ext in (".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg"):
            content = load_file_content(
                bucket_cfg.bucket or bucket_name,
                bucket_cfg.endpoint,
                bucket_cfg.get("access_key"),
                bucket_cfg.get("secret_key"),
                path,
            )
            st.image(content, caption=path)

        elif ext == ".tif":
            m = leafmap.Map()
            m.add_cog_layer(http_path, palette="viridis", name="Remote COG")
            m.to_streamlit()

        elif ext == ".parquet":
            info = None
            # Read and display metadata using fiona
            try:
                info = read_info(http_path)
            except Exception as meta_err:
                st.info(f"Could not read metadata with PyOGRIO: {meta_err}")

            if info:
                with st.expander("Show file metadata (PyOGRIO)"):
                    st.json(info)

            if info["crs"]:
                try:
                    with fs.open(s3_path, "rb") as f:
                        gdf = ga.to_geopandas(read_geoparquet_table(f)).head(2000)

                    st.warning("Only the first 2000 elements are previewed")
                    m = leafmap.Map()
                    geom_only = gdf[[gdf.geometry.name]]
                    m.add_gdf(geom_only, layer_name="Geometries")
                    st.dataframe(gdf, width="stretch", height=400)
                    try:
                        m.to_streamlit()
                    except Exception as e:
                        st.warning(f"Could not render map preview: {e}")
                except Exception as e:
                    st.warning(f"Could not render as geospatial data: {e}")
            else:
                with fs.open(s3_path, "rb") as f:
                    table = pq.read_table(f)
                st.dataframe(table, width="stretch", height=400)

        elif ext in (".txt", ".md", ".json", ".yaml", ".yml", ".csv", ".xml"):
            content = load_file_content(
                bucket_cfg.bucket or bucket_name,
                bucket_cfg.endpoint,
                bucket_cfg.get("access_key"),
                bucket_cfg.get("secret_key"),
                path,
            )
            st.code(content.decode("utf-8"), language=ext.lstrip("."))
        else:
            st.warning(f"No preview available for {ext} files")
            content = load_file_content(
                bucket_cfg.bucket or bucket_name,
                bucket_cfg.endpoint,
                bucket_cfg.get("access_key"),
                bucket_cfg.get("secret_key"),
                path,
            )
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

    # Path search in sidebar
    path_search = st.sidebar.text_input("Search path", placeholder="Filter by path...")

    if selected_bucket:
        bucket_cfg = cfg.buckets[selected_bucket]

        # Clear cache button for selected bucket
        if st.sidebar.button("Refresh bucket"):
            load_bucket_contents.clear(
                bucket_cfg.bucket or selected_bucket,
                bucket_cfg.endpoint,
                bucket_cfg.get("access_key"),
                bucket_cfg.get("secret_key"),
            )

        with st.spinner("Loading bucket contents..."):
            table = load_bucket_contents(
                bucket_cfg.bucket or selected_bucket,
                bucket_cfg.endpoint,
                bucket_cfg.get("access_key"),
                bucket_cfg.get("secret_key"),
            )

            if table:
                # Filter by path search
                if path_search:
                    path_col = table.column("path")
                    mask = [
                        path_search.lower() in (p.as_py() or "").lower()
                        for p in path_col
                    ]
                    table = table.filter(pa.array(mask))

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
                        f"Preview: {bucket_cfg.endpoint}/"
                        f"{bucket_cfg.bucket or selected_bucket}/{selected_path}"
                    )
                    preview_file(
                        bucket_cfg,
                        bucket_cfg.bucket or bucket_cfg.bucket or selected_bucket,
                        selected_path,
                    )
            else:
                st.info("Bucket is empty")


if __name__ == "__main__":
    run_app()
