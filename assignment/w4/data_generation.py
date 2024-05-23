import random
import csv
from faker import Faker

fake = Faker()

# Generating data for customers table
customers_data = []
for i in range(101, 111):
    customers_data.append((i, fake.first_name(), fake.last_name(), fake.email(), fake.password(), fake.street_address(), fake.city(), fake.state_abbr(), fake.zipcode()))

# Generating data for orders table
orders_data = []
for i in range(1, 1001):
    order_id = i
    order_date = fake.date_between(start_date='-1y', end_date='today')
    order_customer_id = random.randint(101, 110)
    order_status = random.choice(['shipped', 'processing', 'delivered'])
    orders_data.append((order_id, order_date, order_customer_id, order_status))

# Generating data for order_items table
order_items_data = []
for i in range(1, 1001):
    order_item_id = i
    order_id = i
    order_item_product_id = random.randint(1, 20)
    order_item_quantity = random.randint(1, 5)
    order_item_product_price = round(random.uniform(10, 100), 2)
    order_item_subtotal = order_item_quantity * order_item_product_price
    order_items_data.append((order_item_id, order_id, order_item_product_id, order_item_quantity, order_item_subtotal, order_item_product_price))

# Writing data to CSV files
with open('customers.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(["customer_id", "customer_fname", "customer_lname", "customer_email", "customer_password", "customer_street", "customer_city", "customer_state", "customer_zipcode"])
    writer.writerows(customers_data)

with open('orders.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(["order_id", "order_date", "order_customer_id", "order_status"])
    writer.writerows(orders_data)

with open('order_items.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(["order_item_id", "order_id", "order_item_product_id", "order_item_quantity", "order_item_subtotal", "order_item_product_price"])
    writer.writerows(order_items_data)

print("Data generated successfully!")
