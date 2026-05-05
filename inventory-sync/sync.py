import csv
import os
import shutil
import time
from datetime import datetime
from pathlib import Path

import mysql.connector
from mysql.connector import Error
from mysql.connector.constants import ClientFlag
import redis


INPUT_DIR = Path(os.getenv("INPUT_DIR", "/app/input"))
PROCESSED_DIR = Path(os.getenv("PROCESSED_DIR", "/app/processed"))
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "5"))

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "mysql-db"),
    "user": os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASSWORD", "root"),
    "database": os.getenv("DB_NAME", "noah_store"),
    "client_flags": [ClientFlag.FOUND_ROWS],
}

redis_client = redis.Redis(host='redis', port=6379, db=0, decode_responses=True)


def log_info(message):
    print(f"[INFO] {message}", flush=True)


def log_warning(message):
    print(f"[WARNING] {message}", flush=True)


def log_error(message):
    print(f"[ERROR] {message}", flush=True)


def get_db_connection():
    while True:
        try:
            connection = mysql.connector.connect(**DB_CONFIG)
            log_info("Connected to MySQL successfully.")
            return connection
        except Error as error:
            log_warning(f"MySQL is not ready. Retry in 5 seconds. Detail: {error}")
            time.sleep(5)


def validate_row(row, line_number):
    try:
        product_id_raw = row.get("product_id")
        quantity_raw = row.get("quantity")

        if product_id_raw is None or quantity_raw is None:
            raise ValueError("Missing product_id or quantity")

        product_id = int(str(product_id_raw).strip())
        quantity = int(str(quantity_raw).strip())

        if quantity < 0:
            log_warning(
                f"Line {line_number} skipped: quantity < 0. "
                f"product_id={product_id}, quantity={quantity}"
            )
            return None

        return product_id, quantity

    except Exception as error:
        log_warning(f"Line {line_number} skipped: invalid row {row}. Detail: {error}")
        return None


def process_csv_file(file_path):
    connection = get_db_connection()
    cursor = connection.cursor()

    processed_records = 0
    skipped_records = 0

    try:
        with open(file_path, mode="r", encoding="utf-8-sig", newline="") as csv_file:
            reader = csv.DictReader(csv_file)

            if reader.fieldnames is None:
                log_warning(f"{file_path.name} has no header.")
                return False

            required_columns = {"product_id", "quantity"}
            current_columns = set(reader.fieldnames)

            if not required_columns.issubset(current_columns):
                log_warning(
                    f"{file_path.name} has invalid columns. "
                    f"Required: {required_columns}, Found: {current_columns}"
                )
                return False

            for line_number, row in enumerate(reader, start=2):
                validated = validate_row(row, line_number)

                if validated is None:
                    skipped_records += 1
                    continue

                product_id, quantity = validated

                cursor.execute(
                    """
                    UPDATE products
                    SET stock = %s
                    WHERE id = %s
                    """,
                    (quantity, product_id),
                )

                if cursor.rowcount == 0:
                    log_warning(f"Line {line_number}: product_id={product_id} not found.")
                    skipped_records += 1
                    continue

                try:
                    redis_client.set(f"product:{product_id}:stock", quantity)
                except Exception as e:
                    log_warning(f"Failed to set stock in Redis for product_id={product_id}: {e}")

                processed_records += 1

        connection.commit()

        log_info(
            f"Processed {processed_records} records. "
            f"Skipped {skipped_records} invalid records."
        )

        return True

    except Exception as error:
        connection.rollback()
        log_error(f"Cannot process file {file_path.name}. Detail: {error}")
        return False

    finally:
        cursor.close()
        connection.close()


def move_file_to_processed(file_path):
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    new_filename = f"{file_path.stem}_{timestamp}.csv"
    destination = PROCESSED_DIR / new_filename

    shutil.move(str(file_path), str(destination))
    log_info(f"Moved file to processed folder: {destination}")


def scan_input_folder():
    INPUT_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    csv_files = sorted(INPUT_DIR.glob("*.csv"))

    for csv_file in csv_files:
        processing_file = csv_file.with_suffix(csv_file.suffix + ".processing")

        try:
            csv_file.rename(processing_file)
            log_info(f"Found file: {csv_file.name}")

            success = process_csv_file(processing_file)

            if success:
                move_file_to_processed(processing_file)
            else:
                original_file = INPUT_DIR / csv_file.name
                processing_file.rename(original_file)
                log_warning(f"File returned to input folder: {original_file}")

        except Exception as error:
            log_error(f"Unexpected error while handling {csv_file.name}. Detail: {error}")


def main():
    log_info("Legacy Adapter started.")
    log_info(f"Input folder: {INPUT_DIR}")
    log_info(f"Processed folder: {PROCESSED_DIR}")
    log_info(f"Polling every {POLL_SECONDS} seconds.")

    while True:
        scan_input_folder()
        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()