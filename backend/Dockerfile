FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libpq-dev \
    poppler-utils \
    tesseract-ocr \
    libtesseract-dev \
    libglib2.0-0 \
    libgl1 \
    libglu1-mesa \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Expose port
EXPOSE 8000

# Run application
CMD ["sh", "-c", "uvicorn main:app --host 0.0.0.0 --port 8000 & python3 ./app/pipeline/schedule.py"]

# sync 1774962858315999969
# sync 1774962859613851218
# sys_sync_57d25f2c
# sys_sync_63b87cac
