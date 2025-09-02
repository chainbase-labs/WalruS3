# WalruS3

A lightweight S3-compatible object storage service using Walrus as the backend storage engine and PostgreSQL for metadata management.

https://github.com/user-attachments/assets/45e32a79-ee95-4dfe-a060-f22edb320671

## Features

- S3 API compatibility
- Scalable object storage with Walrus backend
- Robust metadata management using PostgreSQL
- Support for basic S3 operations (PUT, GET, DELETE, LIST)
- Modern browser interface for viewing buckets and files
- Seamless data migration from S3-compatible storage to Walrus

## Usage

WalruS3 offers three core functionalities:

- **API Service**: Provides S3-compatible endpoints for object storage operations
- **Web UI**: Modern browser interface for viewing buckets and files
- **Data Migration**: Facilitates the transfer of data from other S3-compatible storage services into WalruS3

Please note: The Data Migration tool requires the API Service to be running.

To get started, follow the steps below:
### Prerequisites

1. Clone the repository:

```bash
git clone https://github.com/chainbase-labs/WalruS3.git
cd WalruS3
```

2. Create Docker network:

```bash
docker network create walrus3-network
```

3. Start PostgreSQL database:

```bash
docker run -d \
  --name walrus3-postgres \
  --network walrus3-network \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_DB=walrus3 \
  -v walrus3_postgres_data:/var/lib/postgresql/data \
  postgres:16.4
```

4. Build the WalruS3 Docker image:

```bash
docker build -t walrus3 .
```

### API Service

Start the WalruS3 API service to provide S3-compatible endpoints:

**For Testnet (Default):**
```bash
# Start server on testnet (no publisher/aggregator needed)
docker run -d \
  --name walrus3-server \
  --network walrus3-network \
  -p 9000:9000 \
  walrus3 server \
  -pg.host walrus3-postgres \
  -pg.port 5432 \
  -pg.user postgres \
  -pg.password postgres \
  -pg.dbname walrus3
```

**For Mainnet:**
```bash
# Start server on mainnet (requires mainnet publisher/aggregator configuration)
docker run -d \
  --name walrus3-server \
  --network walrus3-network \
  -p 9000:9000 \
  walrus3 server \
  -pg.host walrus3-postgres \
  -pg.port 5432 \
  -pg.user postgres \
  -pg.password postgres \
  -pg.dbname walrus3 \
  -publisher http://{your-walrus-mainnet-publisher-ip}:{port} \
  -aggregator http://{your-walrus-mainnet-aggregator-ip}:{port}
```

### Web UI

**Access the Web UI:**
The Web UI will be available at the same endpoints as mentioned above. Once the server is running, you can access the modern Web UI by opening your browser and navigating to:
- **http://localhost:9000** - The server automatically detects browser requests and serves the Web UI

<div>
<img src="/static/ui_home.png" width="830" height="500" alt="Home UI">
</div>

<div>
<img src="/static/ui_bucket.png" width="830" height="500" alt="Bucket UI">
</div>


### Data Migration

Migrate data from any S3-compatible storage to WalruS3:

```bash
# Migrate from AWS S3 or other S3-compatible services
docker run --rm \
  --network walrus3-network \
  walrus3 migration \
  -source.endpoint s3.amazonaws.com \
  -source.access-key YOUR_ACCESS_KEY \
  -source.secret-key YOUR_SECRET_KEY \
  -source.bucket my-source-bucket \
  -dest.endpoint http://walrus3-server:9000 \
  -dest.bucket my-dest-bucket \
  -verbose
```

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
