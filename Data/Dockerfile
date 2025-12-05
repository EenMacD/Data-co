FROM python:3.9-slim

WORKDIR /app

# Install system dependencies
# postgresql-client: for psql command in setup scripts
# tesseract-ocr: for pytesseract
# poppler-utils: for pdf2image
RUN apt-get update && apt-get install -y \
    postgresql-client \
    tesseract-ocr \
    poppler-utils \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first to leverage cache
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy all scripts
COPY . .

# Make scripts executable
RUN chmod +x *.sh

# Keep container running to allow executing scripts via docker exec
CMD ["tail", "-f", "/dev/null"]
