FROM ghcr.io/prefix-dev/pixi:latest

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
