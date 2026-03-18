# S3 Explorer

A Streamlit-based web application for exploring S3-compatible storage buckets and previewing their contents.

## Features

- **Browse S3 Buckets**: Select from configured buckets and view all objects with path, size, and last modified date
- **Search**: Filter objects by path with case-insensitive search
- **File Preview**:
  - **Images**: Preview PNG, JPG, JPEG, GIF, WebP, and SVG files
  - **GeoParquet**: Visualize geospatial data on an interactive Folium map (first 2000 features)
  - **Parquet**: Display tabular data
  - **Text files**: Syntax-highlighted preview for TXT, MD, JSON, YAML, CSV, and XML
  - **Other files**: Download button for unsupported formats
- **Anonymous Access**: Supports S3 buckets without authentication
- **Cache Management**: Refresh bucket contents with a single click

## Setup

Install [pixi](https://pixi.sh/latest/#installation), then:

```bash
pixi install
```

## Configuration

Copy the example config and edit it:

```bash
cp config.example.yaml config.yaml
```

Configure your buckets in `config.yaml`:

```yaml
buckets:
  my-bucket:
    endpoint: https://s3.amazonaws.com
    bucket: actual-bucket-name  # optional, defaults to the key name
    access_key: your-access-key  # optional, omit for anonymous access
    secret_key: your-secret-key
```

You can also set the config path via environment variable:

```bash
export S3_EXPLORER_CONFIG=/path/to/config.yaml
```

## Run

```bash
pixi run streamlit run app.py
```

Then open http://localhost:8501 in your browser.

## Docker

Build and run with Docker:
```bash
docker compose up
```
