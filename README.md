# Data Modeling Pipeline

A reproducible, production-style data modeling pipeline demonstrating end-to-end data engineering practices: Python-based ingestion, PostgreSQL data warehousing, dbt transformations, data quality testing, and fully Dockerized execution.

## Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Data Source   │───▶│    Ingestion    │───▶│   PostgreSQL    │───▶│      dbt        │
│   (CSV/API)     │    │    (Python)     │    │   (Raw Layer)   │    │ (Transformations)│
└─────────────────┘    └─────────────────┘    └─────────────────┘    └─────────────────┘
                                                                              │
                                                                              ▼
                                                                     ┌─────────────────┐
                                                                     │  Analytics-Ready │
                                                                     │     Tables       │
                                                                     └─────────────────┘
```

## Features

- **Modular Ingestion**: Reusable Python components for data extraction with validation
- **Schema Evolution**: Versioned database migrations for reproducible environments
- **dbt Transformations**: Layered modeling (staging → intermediate → marts) with documentation
- **Data Quality**: Built-in tests for schema validation, uniqueness, referential integrity
- **Dockerized**: One-command setup with Docker Compose
- **Idempotent Design**: Safe to re-run without data duplication

## Tech Stack

| Component | Technology |
|-----------|------------|
| Ingestion | Python 3.11, pandas |
| Warehouse | PostgreSQL 15 |
| Modeling | dbt-core 1.7 |
| Orchestration | Docker Compose |
| Testing | dbt tests, pytest |

## Quick Start

### Prerequisites
- Docker & Docker Compose
- (Optional) Python 3.11+ for local development

### Run the Pipeline

```bash
# Clone and start
git clone https://github.com/Svencol/data-modeling-pipeline.git
cd data-modeling-pipeline

# Start all services
docker-compose up -d

# Run ingestion
docker-compose exec ingestion python -m ingestion.main

# Run dbt transformations
docker-compose exec dbt dbt run

# Run data quality tests
docker-compose exec dbt dbt test
```

## Project Structure

```
data-modeling-pipeline/
├── ingestion/                 # Python ingestion module
│   ├── __init__.py
│   ├── main.py               # Entry point
│   ├── extractors/           # Data source extractors
│   │   ├── base.py           # Abstract base extractor
│   │   ├── csv_extractor.py
│   │   └── api_extractor.py
│   ├── loaders/              # Database loaders
│   │   └── postgres_loader.py
│   └── validators/           # Data validation
│       └── schema_validator.py
├── warehouse/                 # Database setup
│   ├── init.sql              # Schema initialization
│   └── migrations/           # Schema versioning
├── dbt_project/              # dbt transformations
│   ├── dbt_project.yml
│   ├── models/
│   │   ├── staging/          # Raw → cleaned
│   │   ├── intermediate/     # Business logic
│   │   └── marts/            # Analytics-ready
│   ├── tests/                # Data quality tests
│   └── macros/               # Reusable SQL
├── data/                     # Sample datasets
├── docker-compose.yml
├── Dockerfile.ingestion
├── Dockerfile.dbt
└── README.md
```

## Data Model

This pipeline processes e-commerce order data through three layers:

### Staging Layer
- `stg_customers`: Cleaned customer records
- `stg_orders`: Cleaned order records  
- `stg_products`: Cleaned product catalog

### Intermediate Layer
- `int_orders_enriched`: Orders joined with customer and product data

### Marts Layer
- `fct_orders`: Fact table with order metrics
- `dim_customers`: Customer dimension with lifetime metrics
- `dim_products`: Product dimension with sales metrics

## Data Quality

Built-in tests validate:
- Primary key uniqueness
- Not-null constraints on required fields
- Referential integrity between tables
- Accepted values for categorical fields
- Custom business logic (e.g., order amounts > 0)

Run tests:
```bash
docker-compose exec dbt dbt test
```

## Development

### Local Setup (without Docker)

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Set environment variables
export POSTGRES_HOST=localhost
export POSTGRES_DB=warehouse
export POSTGRES_USER=pipeline
export POSTGRES_PASSWORD=pipeline

# Run ingestion
python -m ingestion.main

# Run dbt
cd dbt_project
dbt run
dbt test
```

### Adding New Data Sources

1. Create a new extractor in `ingestion/extractors/`
2. Inherit from `BaseExtractor`
3. Implement `extract()` method
4. Add corresponding staging model in dbt

## License

MIT License - see [LICENSE](LICENSE) for details.
