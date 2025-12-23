# Airflow Integration Example

This directory contains a sample Apache Airflow DAG to schedule `mlprep` pipelines.

## Contents
- `mlprep_dag.py`: Defines a DAG with a `BashOperator` to execute the `mlprep` binary.

## Usage
1. Copy `mlprep_dag.py` to your Airflow `dags/` folder.
2. Ensure `mlprep` binary is installed on the worker nodes or accessible via path.
3. Update the paths in the DAG file (`/path/to/data/input.csv` and `/path/to/pipelines/pipeline.yaml`) to match your actual environment.
4. Enable the DAG in the Airflow UI.
