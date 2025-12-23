import pandas as pd
import numpy as np

def generate_data():
    np.random.seed(42)
    n_rows = 100
    
    data = {
        'id': range(1, n_rows + 1),
        'name': [f'User_{i}' for i in range(n_rows)],
        'age': np.random.randint(15, 60, size=n_rows),
        'city': np.random.choice(['Tokyo', 'Osaka', 'Nagoya', 'Fukuoka'], size=n_rows),
        'score': np.random.rand(n_rows) * 100
    }
    
    df = pd.DataFrame(data)
    df.to_csv('data.csv', index=False)
    print("Generated data.csv with 100 rows")

if __name__ == "__main__":
    generate_data()
