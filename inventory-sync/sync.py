import os
import time
import shutil
import pandas as pd
import mysql.connector

INPUT_DIR = '/app/input'
PROCESSED_DIR = '/app/processed'

os.makedirs(INPUT_DIR, exist_ok=True)
os.makedirs(PROCESSED_DIR, exist_ok=True)

def get_db_connection():
    for attempt in range(5):
        try:
            return mysql.connector.connect(
                host=os.getenv('DB_HOST', 'mysql-db'),
                user=os.getenv('DB_USER', 'root'),
                password=os.getenv('DB_PASSWORD', 'root'),
                database=os.getenv('DB_NAME', 'noah_store')
            )
        except:
            time.sleep(5)
    return None

def main():
    conn = get_db_connection()
    if not conn:
        print("[ERROR] Không thể kết nối Database!")
        return

    cursor = conn.cursor()
    print(f"[*] Đang lắng nghe thư mục {INPUT_DIR} 24/7...")

    # ĐÂY LÀ VÒNG LẶP GIÚP CODE KHÔNG BỊ "EXIT CODE 0"
    while True:
        for filename in os.listdir(INPUT_DIR):
            if filename.endswith('.csv'):
                filepath = os.path.join(INPUT_DIR, filename)
                valid_records, invalid_records = 0, 0

                try:
                    df = pd.read_csv(filepath)
                    for index, row in df.iterrows():
                        try:
                            product_id = int(row['product_id'])
                            quantity = int(row['quantity'])
                            if quantity < 0:
                                invalid_records += 1
                                continue
                            
                            cursor.execute("UPDATE products SET stock = stock + %s WHERE id = %s", (quantity, product_id))
                            valid_records += 1
                        except Exception:
                            invalid_records += 1
                            continue
                    
                    conn.commit()
                    print(f"[INFO] Processed {valid_records} records. Skipped {invalid_records} invalid records.")
                    
                    # Dọn dẹp file sang processed
                    shutil.move(filepath, os.path.join(PROCESSED_DIR, f"inventory_{int(time.time())}.csv"))

                except Exception as e:
                    print(f"[ERROR] Không thể đọc file: {e}")

        # Quét xong 1 vòng thì nghỉ 5 giây rồi lặp lại, tuyệt đối không thoát chương trình
        time.sleep(5)

if __name__ == "__main__":
    main()