import random
from datetime import datetime, date, timedelta

import psycopg2
from ingestion.db_config import get_db_config

# -----------------------------
# Database connection
# -----------------------------

config = get_db_config()

connection = psycopg2.connect(
    host=config["host"],
    port=config["port"],
    database=config["dbname"],
    user=config["user"],
    password=config["password"]
)

cursor = connection.cursor()


# -----------------------------
# Sample data
# -----------------------------

first_names = [
    "Arun", "Priya", "Rahul", "Ananya", "Karthik",
    "Meena", "Vikram", "Divya", "Sanjay", "Harini"
]

last_names = [
    "Kumar", "Sharma", "Reddy", "Iyer", "Nair",
    "Patel", "Rao", "Menon", "Das", "Krishnan"
]

cities = [
    "Chennai",
    "Coimbatore",
    "Bangalore",
    "Hyderabad",
    "Mumbai",
    "Pune",
    "Delhi"
]

states = [
    "Tamil Nadu",
    "Karnataka",
    "Telangana",
    "Maharashtra",
    "Delhi"
]

categories = [
    "Electronics",
    "Clothing",
    "Home",
    "Books",
    "Sports"
]

payment_methods = [
    "credit_card",
    "debit_card",
    "upi",
    "net_banking"
]

order_statuses = [
    "pending",
    "confirmed",
    "shipped",
    "delivered",
    "cancelled"
]


# -----------------------------
# Generate customers
# -----------------------------

cursor.execute("SELECT COUNT(*) FROM customers")
customer_count = cursor.fetchone()[0]

if customer_count == 0:

    for i in range(1, 101):

        first_name = random.choice(first_names)
        last_name = random.choice(last_names)

        email = f"customer{i}@shopflow.com"

        city = random.choice(cities)
        state = random.choice(states)

        signup_date = date.today() - timedelta(
            days=random.randint(30, 1000)
        )

        cursor.execute(
            """
            INSERT INTO customers
            (first_name, last_name, email, city, state, country, signup_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                first_name,
                last_name,
                email,
                city,
                state,
                "India",
                signup_date
            )
        )

    print("100 customers generated.")

else:

    print("Customers already exist. Skipping customer generation.")


# -----------------------------
# Generate products
# -----------------------------

cursor.execute("SELECT COUNT(*) FROM products")
product_count = cursor.fetchone()[0]

if product_count == 0:

    for i in range(1, 51):

        category = random.choice(categories)

        product_name = f"{category} Product {i}"

        unit_price = round(
            random.uniform(100, 50000),
            2
        )

        cost_price = round(
            unit_price * random.uniform(0.5, 0.8),
            2
        )

        supplier = f"Supplier {random.randint(1, 10)}"

        cursor.execute(
            """
            INSERT INTO products
            (product_name, category, subcategory,
             unit_price, cost_price, supplier)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                product_name,
                category,
                f"{category} Subcategory",
                unit_price,
                cost_price,
                supplier
            )
        )

    print("50 products generated.")

else:

    print("Products already exist. Skipping product generation.")


connection.commit()


# -----------------------------
# Generate orders
# -----------------------------

cursor.execute("SELECT COUNT(*) FROM orders")
order_count = cursor.fetchone()[0]

if order_count == 0:
    for i in range(1, 301):
        customer_id = random.randint(1, 100)

        # Get the selected customer's signup date
        cursor.execute(
            """
            SELECT signup_date
            FROM customers
            WHERE customer_id = %s
            """,
            (customer_id,)
        )

        signup_date = cursor.fetchone()[0]

        # Generate an order date on or after signup date
        signup_datetime = datetime.combine(
            signup_date,
            datetime.min.time()
        )

        current_datetime = datetime.now()

        seconds_since_signup = int(
            (current_datetime - signup_datetime).total_seconds()
        )

        order_date = signup_datetime + timedelta(
            seconds=random.randint(0, seconds_since_signup)
        )

        order_status = random.choice(order_statuses)

        shipping_city = random.choice(cities)
        shipping_state = random.choice(states)

        cursor.execute(
            """
            INSERT INTO orders
            (customer_id, order_date, order_status,
             shipping_city, shipping_state)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING order_id
            """,
            (
                customer_id,
                order_date,
                order_status,
                shipping_city,
                shipping_state
            )
        )

        order_id = cursor.fetchone()[0]

        # -----------------------------
        # Generate order items
        # -----------------------------

        number_of_items = random.randint(1, 4)

        for _ in range(number_of_items):

            product_id = random.randint(1, 50)

            cursor.execute(
                """
                SELECT unit_price
                FROM products
                WHERE product_id = %s
                """,
                (product_id,)
            )

            unit_price = cursor.fetchone()[0]

            quantity = random.randint(1, 5)

            discount = round(
                random.uniform(0, 500),
                2
            )

            cursor.execute(
                """
                INSERT INTO order_items
                (order_id, product_id, quantity,
                 unit_price, discount)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (
                    order_id,
                    product_id,
                    quantity,
                    unit_price,
                    discount
                )
            )

    print("300 orders and their order items generated.")

else:

    print("Orders already exist. Skipping order generation.")


connection.commit()


# -----------------------------
# Generate payments
# -----------------------------

cursor.execute("SELECT COUNT(*) FROM payments")
payment_count = cursor.fetchone()[0]

if payment_count == 0:

    for order_id in range(1, 301):

        cursor.execute(
            """
            SELECT
                SUM((quantity * unit_price) - discount)
            FROM order_items
            WHERE order_id = %s
            """,
            (order_id,)
        )

        amount = cursor.fetchone()[0]

        payment_method = random.choice(payment_methods)

        payment_status = random.choice([
            "completed",
            "completed",
            "completed",
            "pending",
            "failed",
            "refunded"
        ])

        payment_date = datetime.now() - timedelta(
            days=random.randint(0, 365)
        )

        cursor.execute(
            """
            INSERT INTO payments
            (order_id, payment_method,
             payment_status, amount, payment_date)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (
                order_id,
                payment_method,
                payment_status,
                amount,
                payment_date
            )
        )

    print("300 payments generated.")

else:

    print("Payments already exist. Skipping payment generation.")


connection.commit()


# -----------------------------
# Close connection
# -----------------------------

cursor.close()
connection.close()

print("ShopFlow source data generation completed.")