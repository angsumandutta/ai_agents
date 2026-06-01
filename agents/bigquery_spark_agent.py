import os
import json
import time
import subprocess
from google.cloud import bigquery
from google.oauth2.credentials import Credentials
from google.oauth2 import service_account
from pyspark.sql import SparkSession


class BigQuerySparkAgent:
    """
    Simple AI Agent which connects to BigQuery using Spark BigQuery connector.

    This is intentionally written in a way that DataBuck scanner can detect:
    - agent name
    - datasource type
    - BigQuery project
    - dataset
    - table
    - query
    """

    def __init__(self):
        self.agent_name = "BigQuery Customer Insight Agent"
        self.datasource_type = "bigquery"

        # Update these values based on your BigQuery setup
        self.project = "databuckk8s"
        self.parent_project = "demoprojectfe"
        self.materialization_project = "newdemoproject-412013"

        self.dataset = "new_data_set"
        self.materialization_dataset = "Test_Views"
        self.table_name = "Cruise_Policy_Driven_Dataset"

        # KMS flag logic similar to your DataBuck script
        # N = use WIF token
        # Y = use service account JSON
        self.kmsauthdisabled = "Y"

        self.client_id = "your-client-id"

        self.databuck_home = os.getenv("DATABUCK_HOME", "/opt/databuck")

        self.gcp_credentials_path = os.path.join(
            self.databuck_home,
            "propertiesFiles",
            f"{self.project}.json"
        )

        self.wif_token = None

    def get_wif_token(self):
        """
        Same idea as your script:
        Reuse cached token if valid, otherwise call runAccessTokenAuth.sh.
        """

        token_file = os.path.join(
            self.databuck_home,
            "logs",
            "gcp_wif_monitoring",
            self.client_id,
            f"gcp_wif_token_status_{self.client_id}.txt"
        )

        if os.path.exists(token_file):
            try:
                with open(token_file, "r") as file:
                    lines = file.read().splitlines()

                if len(lines) >= 2:
                    token = lines[0].strip()
                    expiry = int(lines[1].strip())
                    now = int(time.time())

                    if expiry - now > 300:
                        print("Reusing cached WIF token")
                        return token

                    print("Token near expiry, regenerating")

            except Exception as error:
                print(f"Error reading WIF token file: {error}")

        script_path = os.path.join(
            self.databuck_home,
            "scripts",
            "runAccessTokenAuth.sh"
        )

        output = subprocess.check_output(
            ["bash", script_path, self.client_id]
        ).decode("utf-8")

        for line in output.splitlines():
            if line.startswith("Final_Access_Token##"):
                token = line.split("##")[1].strip()
                print("Generated new WIF token")
                return token

        raise Exception("Failed to get WIF token")

    def get_bigquery_client(self):
        """
        Same logic as your script:
        - If KMS disabled flag is N, use WIF token
        - Else use service account JSON
        """

        if self.kmsauthdisabled == "N":
            self.wif_token = self.get_wif_token()
            credentials = Credentials(token=self.wif_token)

        else:
            if not os.path.exists(self.gcp_credentials_path):
                raise FileNotFoundError(
                    f"Service account JSON not found: {self.gcp_credentials_path}"
                )

            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = self.gcp_credentials_path
            credentials = service_account.Credentials.from_service_account_file(
                self.gcp_credentials_path
            )

        return bigquery.Client(
            project=self.project,
            credentials=credentials
        )

    def get_partition_filter_sql(self, bq_client):
        """
        Same partition handling style from your script.
        """

        full_table_name = f"{self.project}.{self.dataset}.{self.table_name}"
        table = bq_client.get_table(full_table_name)

        if table.time_partitioning:
            field = table.time_partitioning.field

            if field:
                return f"{field} IS NOT NULL"

            return "_PARTITIONDATE IS NOT NULL"

        if table.range_partitioning:
            field = table.range_partitioning.field
            return f"{field} IS NOT NULL"

        return None

    def create_spark_session(self):
        """
        Create Spark session with BigQuery connector jar.
        This follows your DataBuck Spark BigQuery connector pattern.
        """

        jars = ",".join([
            f"{self.databuck_home}/jars/spark-bigquery-with-dependencies_2.12-0.43.1.jar"
        ])

        spark = (
            SparkSession.builder
            .master("local[*]")
            .appName("BigQueryCustomerInsightAgent")
            .config("spark.jars", jars)
            .config("spark.driver.memory", "5g")
            .getOrCreate()
        )

        return spark

    def build_sample_query(self, partition_filter):
        """
        Build sample query like your existing DataBuck script.
        """

        full_table_name = f"{self.project}.{self.dataset}.{self.table_name}"

        if partition_filter:
            query = f"""
            SELECT *
            FROM `{full_table_name}`
            WHERE {partition_filter}
              AND RAND() < 0.03
            LIMIT 50
            """
        else:
            query = f"""
            SELECT *
            FROM `{full_table_name}`
            WHERE RAND() < 0.03
            LIMIT 50
            """

        return query

    def read_bigquery_dataframe(self):
        """
        Main method:
        - Create BigQuery client
        - Get partition filter
        - Create Spark session
        - Read BigQuery table using Spark connector
        """

        bq_client = self.get_bigquery_client()
        partition_filter = self.get_partition_filter_sql(bq_client)

        print(f"Partition filter: {partition_filter}")

        query = self.build_sample_query(partition_filter)

        spark = self.create_spark_session()

        reader = (
            spark.read.format("bigquery")
            .option("viewsEnabled", "true")
            .option("bigQueryDataTypes", "false")
            .option("materializationProject", self.materialization_project)
            .option("materializationDataset", self.materialization_dataset)
            .option("project", self.project)
            .option("parentProject", self.parent_project)
            .option("query", query)
        )

        if self.kmsauthdisabled == "N":
            reader = reader.option("gcpAccessToken", self.wif_token)
        else:
            reader = reader.option("credentialsFile", self.gcp_credentials_path)

        df_spark = reader.load()

        return spark, df_spark, query

    def run_agent(self):
        """
        Agent execution method.
        """

        spark, df_spark, query = self.read_bigquery_dataframe()

        print("Agent Name:", self.agent_name)
        print("Datasource Type:", self.datasource_type)
        print("Project:", self.project)
        print("Dataset:", self.dataset)
        print("Table:", self.table_name)
        print("Query Used:", query)

        print("Schema:")
        df_spark.printSchema()

        print("Sample Records:")
        df_spark.show(10, truncate=False)

        result = {
            "agent_name": self.agent_name,
            "datasource_type": self.datasource_type,
            "project": self.project,
            "dataset": self.dataset,
            "table_name": self.table_name,
            "query": query,
            "columns": df_spark.columns,
            "record_count_sample": df_spark.count()
        }

        spark.stop()

        return result


if __name__ == "__main__":
    agent = BigQuerySparkAgent()
    summary = agent.run_agent()

    print(json.dumps(summary, indent=2))