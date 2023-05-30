from flask import Flask, jsonify, request
from datetime import datetime, date
import psycopg2

app = Flask(__name__)

# Define database connection parameters
db_host = "127.0.0.1"
db_port = "5432"
db_name = '' # your db name
db_user = '' # your db user
db_password = '' # your db password

# Endpoint for customers with birthdays today


@app.route("/customers/birthday")
def customers_with_birthday_today():
    # Connect to the database
    conn = psycopg2.connect(
        host=db_host,
        port=db_port,
        dbname=db_name,
        user=db_user,
        password=db_password
    )
    cursor = conn.cursor()

    # Get today's date
    today = date.today()

    # Query the database for customers with birthdays today
    query = f"SELECT customer_id, customer_first_name FROM customers WHERE EXTRACT(month FROM birthdate::date) = {today.month} AND EXTRACT(day FROM birthdate::date) = {today.day}"
    cursor.execute(query)
    results = cursor.fetchall()

    # Close the database connection
    cursor.close()
    conn.close()

    # Build response JSON
    customers = [{"customer_id": row[0], "customer_first_name": row[1]}
                 for row in results]
    return jsonify({"customers": customers})

# Endpoint for top selling products for a specific year


@app.route("/products/top-selling-products/<int:year>")
def top_selling_products(year):
    # Connect to the database
    conn = psycopg2.connect(
        host=db_host,
        port=db_port,
        dbname=db_name,
        user=db_user,
        password=db_password
    )
    cursor = conn.cursor()

    # Query the database for top selling products in the given year
    query = f"""
    SET datestyle = dmy;
    SELECT p.product, SUM(o.quantity_sold::integer) AS total_sales
    FROM pastry_inventory o
    JOIN products p ON o.product_id = p.product_id
    WHERE EXTRACT(YEAR FROM TO_DATE(o.transaction_date, 'MM/DD/YYYY')) = {year}
    GROUP BY p.product
    ORDER BY total_sales DESC
    LIMIT 10;
"""
    cursor.execute(query)
    results = cursor.fetchall()

    # Close the database connection
    cursor.close()
    conn.close()

    # Build response JSON
    products = [{"product_name": row[0], "total_sales": row[1]}
                for row in results]
    return jsonify({"products": products})

# Endpoint for last order per customer


@app.route("/customers/last-order-per-customer")
def last_order_per_customer():
    # Connect to the database
    conn = psycopg2.connect(
        host=db_host,
        port=db_port,
        dbname=db_name,
        user=db_user,
        password=db_password
    )
    cursor = conn.cursor()

    # Query the database for the last order per customer
    query = """
    SELECT o.customer_id, c.customer_email, MAX(o.transaction_date) AS last_order_date
    FROM sales_reciepts o
    JOIN customers c ON o.customer_id = c.customer_id
    GROUP BY o.customer_id, c.customer_email;
    """
    cursor.execute(query)
    results = cursor.fetchall()

    # Close the database connection
    cursor.close()
    conn.close()

    # Build response JSON
    customers = [{"customer_id": row[0], "customer_email": row[1],
                  "last_order_date": row[2]} for row in results]
    return jsonify({"customers": customers})


if __name__ == '__main__':
    app.run(port=8000)
