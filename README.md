# 💡 Airflow DAGs

Welcome to my ever-growing playground of Apache Airflow DAGs - a space where I explore, prototype, and refine real-world data workflows across e-commerce, finance, wellness, and beyond..

## 🧪 What’s Inside

A mix of practical DAGs and experimental scrapes, including:

| DAG Name                | Description                                                     |
|------------------------|-----------------------------------------------------------------|
| `amazon_bestsellers`   | Scrapes Amazon's top-selling products by category daily         |
| `aliexpress_trends`    | Ingests AliExpress popular listings by keyword or category       |
| `crypto_price_tracker` | Tracks top cryptocurrencies and percent change in real-time      |
| `us_stock_market`      | Pulls US stock data (e.g. S&P 500, NASDAQ) for dashboarding       |
| `air_pollution_reverse`| Reconstructs UK air quality dataset from open APIs               |
| `self_confidence_quotes`| Loads curated inspirational quotes into a DB for wellness apps  |
|`vehicleDB_pipeline`   | Tracks latest vehicle models from API by type and model year     |

..and more..

> New pipelines are added regularly - the goal is to blend learning with real-world relevance.

## 🛠️ Tech Stack (Open Source Vulnerabilities)


| Component        | Purpose                        |
|------------------|--------------------------------|
| Python           | Core ETL logic and scripting               |
| Requests         | HTTP client for API interaction                |
| Pandas           | Data wrangling, transformation, and cleaning   |
| SQLAlchemy       | Database ORM and connectivity abstraction           |
| PostgreSQL       | Data warehouse / structured data storage               |
| Apache Airflow   | Workflow orchestration and DAG scheduling |
| Metabase       	 | Business Intelligence (BI) & data visualization |
| Linus	Server     | Hosting / runtime environment |
| Github	         | Version control and CI/CD pipelines                              |
| Docker           | Containerization of services and dependencies for consistent runtime environments; enables isolated, reproducible deployments |

