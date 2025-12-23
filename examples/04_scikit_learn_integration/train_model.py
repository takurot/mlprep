import pandas as pd
import numpy as np
import subprocess
import os
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score

def generate_data():
    np.random.seed(42)
    n_rows = 200
    df = pd.DataFrame({
        'feature1': np.random.normal(0, 1, n_rows),
        'feature2': np.random.normal(5, 2, n_rows),
        'feature3': np.random.choice(['A', 'B'], n_rows), # Ignored in features section for now
        'target': np.random.randint(0, 2, n_rows)
    })
    df.to_csv('raw_data.csv', index=False)
    print("Generated raw_data.csv")

def run_mlprep():
    print("Running mlprep pipeline...")
    # Assumes mlprep is installed and in PATH, or running via cargo
    # Using 'mlprep' assuming it's in the environment. 
    # If developing, one might use 'cargo run --release -- run ...'
    
    # Check if 'mlprep' command exists, else try cargo
    cmd = ["mlprep", "run", "pipeline.yaml"]
    try:
        subprocess.run(cmd, check=True)
    except FileNotFoundError:
        print("'mlprep' command not found, trying 'cargo run --release ...'")
        subprocess.run(["cargo", "run", "--release", "--bin", "mlprep", "--", "run", "pipeline.yaml"], check=True)

def train_model():
    print("Loading processed data...")
    # Read parquet output from mlprep
    df = pd.read_parquet('processed_train.parquet')
    
    X = df[['feature1', 'feature2']]
    y = df['target']
    
    print("Training Logistic Regression...")
    model = LogisticRegression()
    model.fit(X, y)
    
    preds = model.predict(X)
    acc = accuracy_score(y, preds)
    print(f"Model Accuracy: {acc:.4f}")

if __name__ == "__main__":
    generate_data()
    run_mlprep()
    train_model()
