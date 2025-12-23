import mlflow
import subprocess
import os
import pandas as pd
import numpy as np

def generate_data():
    df = pd.DataFrame(np.random.rand(10, 2), columns=['col1', 'col2'])
    df.to_csv('raw_data.csv', index=False)

def run_experiment():
    mlflow.set_experiment("mlprep_experiment")
    
    with mlflow.start_run():
        print("MLflow run started")
        
        # Log Pipeline Config
        mlflow.log_artifact("pipeline.yaml")
        
        # Run mlprep
        print("Running mlprep...")
        cmd = ["mlprep", "run", "pipeline.yaml"]
        try:
            subprocess.run(cmd, check=True)
            run_status = "SUCCESS"
        except (subprocess.CalledProcessError, FileNotFoundError):
            # Fallback for dev environment
            try:
                subprocess.run(["cargo", "run", "--release", "--bin", "mlprep", "--", "run", "pipeline.yaml"], check=True)
                run_status = "SUCCESS"
            except subprocess.CalledProcessError:
                run_status = "FAILED"
        
        mlflow.log_param("status", run_status)
        
        if run_status == "SUCCESS":
            # Log Output Artifact
            if os.path.exists("output.parquet"):
                mlflow.log_artifact("output.parquet")
                print("Logged output.parquet")
            else:
                print("output.parquet not found")
        
        print(f"Run finished with status: {run_status}")

if __name__ == "__main__":
    generate_data()
    run_experiment()
