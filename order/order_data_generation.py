import pandas as pd
import random
from datetime import datetime, timedelta

# Function to generate random dates within a range
def random_date(start_date, end_date):
    delta = end_date - start_date
    random_days = random.randint(0, delta.days)
    return start_date + timedelta(days=random_days)

# Generate random data
data = []
for order_id in range(1, 1001):
    date = random_date(datetime(2020, 1, 1), datetime(2020, 12, 31)).strftime('%d/%m/%Y')
    customer_id = random.randint(3000, 5000)
    status = random.choice(["PENDING", "CLOSED", "COMPLETE"])
    data.append([order_id, date, customer_id, status])

# Create DataFrame
df = pd.DataFrame(data, columns=['order_id', 'date', 'customer_id', 'status'])

# Print the first few rows of the DataFrame
print(df.head())


df.to_csv("orders.csv")