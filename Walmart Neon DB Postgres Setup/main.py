import os

def main():
    print("Hello from walmart-de-project!")


if __name__ == "__main__":
    main()

# Retrieve connection from environment variable or Airflow / Databricks secret
POSTGRES_connection = os.getenv("POSTGRES_CONN", "postgresql://user:password@host/database?sslmode=require")

