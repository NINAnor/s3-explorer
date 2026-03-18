FROM ghcr.io/prefix-dev/pixi:bullseye

RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    apt-get update && apt-get install -y ca-certificates

WORKDIR /app

# Copy project files
COPY pyproject.toml pixi.lock ./
COPY app.py ./

# Install dependencies
RUN pixi install --frozen

# Expose Streamlit port
EXPOSE 8501

# Run Streamlit
CMD ["pixi", "run", "streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
