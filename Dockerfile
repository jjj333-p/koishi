FROM python:3.13.5-slim

WORKDIR /app

COPY requirements.txt .

RUN apt-get update && \
    apt-get install -y  \
    build-essential  \
    libpq-dev && \
    rm -rf /var/lib/apt/lists/*


RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "main.py"]


