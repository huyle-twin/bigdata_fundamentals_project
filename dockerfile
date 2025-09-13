# Base Kestra
FROM kestra/kestra:latest

USER root

# Cài Python + công cụ build
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 python3-pip python3-venv python3-dev \
    gcc build-essential git curl ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# Cài các thư viện Python cần thiết:
RUN pip3 install --no-cache-dir \
    pyspark==3.5.0 \
    word2number==1.1 \
    dbt-core==1.8.* \
    dbt-bigquery==1.8.* \
    google-cloud-bigquery==3.* \
    google-cloud-bigquery-storage==2.* \
    pyarrow==16.*

# Thiết lập môi trường & thư mục
ENV PATH="/usr/local/bin:${PATH}"
WORKDIR /app

# Tạo sẵn thư mục (compose sẽ mount đè)
RUN mkdir -p /app/dbt /app/scripts /app/data && \
    chown -R kestra:kestra /app

USER kestra
