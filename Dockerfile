FROM python:3.11-slim

# System deps: Tesseract for OCR
RUN apt-get update && apt-get install -y --no-install-recommends \
    tesseract-ocr \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py /app/main.py
ENV OMP_THREAD_LIMIT=1
ENTRYPOINT ["python","-u","/app/pdf_text_extract.py"]
