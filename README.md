# WalruS3

A lightweight S3-compatible object storage service using Walrus as the backend storage engine and PostgreSQL for metadata management.

## Features

- S3 API compatibility
- Scalable object storage with Walrus backend
- Robust metadata management using PostgreSQL
- Support for basic S3 operations (PUT, GET, DELETE, LIST)

## Usage

1. Clone the repository:

```
git clone https://github.com/chainbase-labs/WalruS3.git
cd WalruS3
```

2. Start the services:

```
docker-compose up -d
```

This will start:

- WalruS3 service on port 9000

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
