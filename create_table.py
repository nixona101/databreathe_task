import psycopg2
import os
import glob
import csv


def infer_data_type(value):
    try:
        int(value)
        return 'INT'
    except ValueError:
        pass

    try:
        float(value)
        return 'FLOAT'
    except ValueError:
        pass

    return 'VARCHAR(255)'  # Default data type


def ingest_csv_files(csv_directory, db_host, db_port, db_name, db_user, db_password):
    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(
            host=db_host,
            port=db_port,
            dbname=db_name,
            user=db_user,
            password=db_password
        )
        cursor = conn.cursor()

        # Get a list of all CSV files in the directory
        csv_files = glob.glob(os.path.join(csv_directory, '*.csv'))
        # Iterate over each CSV file
        for csv_file in csv_files:
            # Get the base file name without extension
            table_name = os.path.splitext(os.path.basename(csv_file))[0]

            # Read the CSV file and infer data types from the header
            with open(csv_file, 'r') as f:
                reader = csv.reader(f)
                header = next(reader)  # Get the header line
                data_types = [infer_data_type(value) for value in header]

            # Create the table in the database
            columns = [f"{col} {data_type}" for col,
                       data_type in zip(header, data_types)]
            create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(columns)})"
            cursor.execute(create_table_query)
            conn.commit()

            # Copy the data from the CSV file to the database table
            with open(csv_file, 'r') as file:
                reader = csv.reader(file)
                header = next(reader)  # Get header row

                # Prepare the INSERT query
                columns = ', '.join(header)
                placeholders = ', '.join(['%s'] * len(header))
                insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
                for row in reader:
                    if ',' in row[0]:
                        row = row[0].split(",")

                    cursor.execute(insert_query, row)
                    conn.commit()

        conn.commit()
        print("CSV files ingested successfully!")

    except (Exception, psycopg2.Error) as error:
        print("Error while ingesting CSV files into PostgreSQL:", error)

    finally:
        # Close the database connection
        if conn:
            cursor.close()
            conn.close()


# Usage example:
csv_directory = 'mapping/'
db_host = 'localhost'
db_port = '5432'
db_name = ''  # your db name
db_user = ''  # your db user
db_password = ''  # your db password

ingest_csv_files(csv_directory, db_host, db_port,
                 db_name, db_user, db_password)
