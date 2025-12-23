import pandas as pd
import numpy as np

def generate_train_test():
    np.random.seed(42)
    n_train = 100
    n_test = 20
    
    cities = ['Tokyo', 'Osaka', 'Nagoya', 'Fukuoka']
    
    # Train Data
    train = pd.DataFrame({
        'id': range(1, n_train + 1),
        'age': np.random.randint(20, 60, size=n_train),
        'income': np.random.randint(300, 1000, size=n_train) * 10000,
        'city': np.random.choice(cities, size=n_train)
    })
    
    # Test Data (with potentially unseen categories if not handled, but here we keep simple)
    test = pd.DataFrame({
        'id': range(n_train + 1, n_train + n_test + 1),
        'age': np.random.randint(20, 60, size=n_test),
        'income': np.random.randint(300, 1000, size=n_test) * 10000,
        'city': np.random.choice(cities, size=n_test)
    })
    
    train.to_csv('train_data.csv', index=False)
    test.to_csv('test_data.csv', index=False)
    print("Generated train_data.csv and test_data.csv")

if __name__ == "__main__":
    generate_train_test()
