# Watchdog Scraper Service

## Overview
The Watchdog Scraper Service is designed as a continuously running microservice that scrapes website content, processes it, and sends the results to Apache Pulsar. It is particularly useful for identifying malicious or scam websites, such as cryptocurrency-related fraud, by extracting structured data from various domains in real-time.

The service retrieves domains from an Apache Pulsar broker, scrapes the content using Playwright, processes the content (using methods like `HtmlSublimator` for extracting readable text, detecting forms, etc.), and then sends the processed data back to Pulsar.

## Features
- **Domain Scraping**: Collects website content using Playwright.
- **Data Processing**: Extracts various pieces of information (e.g., text, form detection) from HTML using `HtmlSublimator`.
- **Scalable Architecture**: Designed to run continuously, processing domains from a Pulsar queue in parallel.
- **Pulsar Integration**: Uses Apache Pulsar for both consuming domains and publishing results.
- **Logging**: Provides rotating logs to ensure no out-of-control log growth.

## System Requirements
- Python 3.10
- Docker
- Apache Pulsar

### Dependencies
The main dependencies for the Watchdog Scraper Service are listed in `requirements.txt` and include:
- **Playwright**: For scraping websites.
- **BeautifulSoup4**: For parsing HTML.
- **Simhash**: For generating content similarity hashes.
- **Apache Pulsar**: For message queueing.
- **Zstandard**: For compressing data before sending to Pulsar.

## Setup Instructions
### Prerequisites
- Install Docker if not already installed.
- Install Apache Pulsar and ensure it's running, or use an existing Pulsar broker.
- Set up Python 3.10 environment if running the application locally (without Docker).

### Environment Variables
Create a `.env` file in the root directory with the following content:

```
PULSAR_IP=<Pulsar broker IP address>
PULSAR_PORT=<Pulsar broker port, e.g., 6650>
DOMAIN_TOPIC=<Topic name for incoming domains, e.g., domains-to-scrape>
RESULT_TOPIC=<Topic name for scraped results, e.g., scraped-results>
CLIENT_NAME=<Unique client name, e.g., giraffe-red-passion>
```

### Docker Setup
To set up and run the service using Docker, follow these steps:

1. **Build the Docker Image**:
   ```sh
   docker build -t watchdog-scraper .
   ```

2. **Run the Docker Container**:
   ```sh
   docker run -d --name watchdog-scraper -v $(pwd)/logs:/app/logs --env-file .env watchdog-scraper
   ```
   - `-v $(pwd)/logs:/app/logs` ensures that the logs are accessible outside the container.
   - `--env-file .env` loads environment variables from the `.env` file.

### Running Locally
If you want to run the application without Docker:

1. **Install Dependencies**:
   ```sh
   pip install -r requirements.txt
   ```

2. **Run the Service**:
   ```sh
   python main.py
   ```

## Usage
The service runs continuously, consuming domains from Pulsar and processing them in parallel. It maintains a fixed number of concurrent scrapers using semaphores to avoid overloading the system.

- **Input**: Domains are provided one at a time via Pulsar.
- **Processing**: Each domain is scraped and analyzed.
- **Output**: The results are serialized, compressed, and sent back to Pulsar.

### Logs
Logs are stored in the `./logs/service.log` file. The log file is configured to rotate, ensuring logs do not grow uncontrollably.

### Example Workflow
1. Pulsar provides a domain via the `DOMAIN_TOPIC` topic.
2. The scraper service scrapes the domain, analyzes the content, and extracts relevant data.
3. The processed data is compressed and sent back to Pulsar via the `RESULT_TOPIC` topic.

## Key Components
- **`main.py`**: The entry point for the service. Manages Pulsar connections and runs the scraping loop.
- **`HtmlSublimator`**: A utility for processing scraped HTML, including extracting readable text, detecting forms, and calculating simhashes.
- **`Data Builder`**: Gathers results from the `HtmlSublimator`, serializes, compresses, and prepares them for Pulsar.

## Development
### Running Tests
You can create unit tests for individual components like `HtmlSublimator`. Ensure you use mock data to verify the scraping and parsing logic without accessing external websites.

### Linting and Code Quality
Make sure to follow Python code quality standards by running tools like:
```sh
flake8 .
```

## Contributing
Feel free to submit issues or pull requests if you have suggestions or improvements.

## License
This project is licensed under the MIT License.

## Contact
For any questions or support, please contact the development team at `faux@tracelon.com`.

