import pandas as pd
import numpy as np

def generate_dirty_data():
    np.random.seed(42)
    n_rows = 100
    
    data = {
        'id': range(1, n_rows + 1),
        'email': [f'user{i}@example.com' for i in range(n_rows)],
        'age': np.random.randint(15, 60, size=n_rows),
        'user_type': np.random.choice(['admin', 'user', 'guest'], size=n_rows)
    }
    
    df = pd.DataFrame(data)
    
    # Introduce bad data
    # Duplicate emails
    df.loc[0, 'email'] = 'duplicate@example.com'
    df.loc[1, 'email'] = 'duplicate@example.com'
    
    # Invalid age
    df.loc[2, 'age'] = -5
    df.loc[3, 'age'] = 150
    
    # Null user_type
    df.loc[4, 'user_type'] = np.nan
    
    df.to_csv('dirty_data.csv', index=False)
    print("Generated dirty_data.csv with 100 rows (including intentional errors)")

if __name__ == "__main__":
    generate_dirty_data()
