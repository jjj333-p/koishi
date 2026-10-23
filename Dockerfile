FROM python:3.13.5

WORKDIR /app

COPY requirements.txt .

RUN apt-get update && \
    apt-get install -y  \
    build-essential  \
    libpq-dev && \
    rm -rf /var/lib/apt/lists/*


RUN pip install -r requirements.txt

COPY . .

CMD ["python", "main.py"]


