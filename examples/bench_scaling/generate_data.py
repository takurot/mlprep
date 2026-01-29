import pandas as pd
import numpy as np
import os

def generate_data():
    n_rows = 10_000_000
    n_cols = 5
    
    data = {
        f"col_{i}": np.random.rand(n_rows) * 1000 for i in range(n_cols)
    }
    
    df = pd.DataFrame(data)
    # Save as Parquet for faster IO
    output_path = os.path.join(os.path.dirname(__file__), "data.parquet")
    df.to_parquet(output_path)
    print(f"Generated {output_path} with {n_rows} rows and {n_cols} columns")

if __name__ == "__main__":
    generate_data()
