# """
# SEC Pipeline Plugin for Apache Airflow

# This plugin provides custom operators for SEC filings ETL pipeline.

# File location: plugins/sec_pipeline/__init__.py
# """

# #from airflow.plugins_manager import AirflowPlugin

# # Import operators to make them available to Airflow
# from sec_pipeline.operators import (
#     check_companies_csv,
#     download_sec_filings,
#     extract_items_from_filings,
#     convert_to_parquet,
#     merge_parquet_files,
#     send_success_notification,
#     send_failure_notification,
#     # cleanup_temp_files
# )


# # class SecPipelinePlugin(AirflowPlugin):
# #     """
# #     Plugin to register SEC pipeline operators with Airflow
# #     """
# #     name = "sec_pipeline"
# #     operators = []  # Custom operators would go here if we had any
# #     hooks = []  # Custom hooks would go here
# #     executors = []
# #     macros = []
# #     admin_views = []
# #     flask_blueprints = []
# #     menu_links = []