# NutellaDB

A persistent key-value pair database with LRU caching and diff-based version control for efficient backup and restore.

## Installation

### Method 1: Using Docker and Docker Compose (Recommended)

1. Clone or download the NutellaDB repository to your local machine:
   ```bash
   git clone <repository-url>
   cd nutelladb
   ```

2. Create a directory for persistent data storage:
   ```bash
   mkdir -p files
   ```

3. Start NutellaDB using Docker Compose:
   ```bash
   docker-compose up startserver
   ```

   This will build the Docker image and start the server on port 3000.

4. To run NutellaDB CLI commands, use:
   ```bash
   docker-compose run nutella <command>
   ```

### Method 2: Building and Running Locally

1. Install Go 1.24 or later:
   ```bash
   # For macOS using Homebrew
   brew install go
   
   # For Ubuntu/Debian
   apt-get update && apt-get install golang-go
   ```

2. Clone or download the NutellaDB repository:
   ```bash
   git clone <repository-url>
   cd nutelladb
   ```

3. Download the required dependencies:
   ```bash
   go mod download
   ```

4. Build the application:
   ```bash
   go build -o nutelladb
   ```

5. Run the server:
   ```bash
   ./nutelladb startserver
   ```
## Getting Help

Run the following command to see available options and commands:

```bash
# Using Docker Compose
docker-compose run nutella help

# Running locally
./nutelladb help
```