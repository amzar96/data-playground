FROM python:3.12.9-bookworm

ENV PYTHONUNBUFFERED 1
ENV GRPC_PYTHON_DISABLE_LIBC_COMPATIBILITY 1

# Update package list dan install dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        bash \
        gcc \
        g++ \
        make \
        cmake \
        git \
        python3-dev \
        zlib1g-dev \
        libssl-dev \
        autoconf \
        automake \
        libtool \
        pkg-config \
        bison \
        flex \
        libgrpc-dev \
        protobuf-compiler && \
    pip install --no-cache-dir --upgrade pip setuptools wheel && \
    pip install --no-cache-dir grpcio

# Copy requirements file
COPY requirements.txt ./

# Install dependencies from requirements.txt
RUN pip install --no-cache-dir --prefer-binary -r requirements.txt || \
    pip install --no-cache-dir --verbose -r requirements.txt

# Copy project files
COPY . .

CMD ["dagster", "dev", "--host", "0.0.0.0", "-p", "3000"]
