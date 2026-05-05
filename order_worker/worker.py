import pika, json, time, mysql.connector, psycopg2

def get_mysql_conn():
    return mysql.connector.connect(host='mysql-db', user='root', password='root', database='noah_store')

def get_postgres_conn():
    return psycopg2.connect(host='postgres-db', user='postgres', password='pg_password', dbname='finance')

def process_order(ch, method, properties, body):
    order = json.loads(body)
    order_id = order['order_id']
    print(f"[>] Đang xử lý đơn hàng ID: {order_id}")

    try:
        # PostgreSQL: Xuất hiện dòng dữ liệu mới
        pg_conn = get_postgres_conn()
        pg_cursor = pg_conn.cursor()
        pg_cursor.execute("INSERT INTO transactions (order_id, status) VALUES (%s, 'SUCCESS')", (order_id,))
        pg_conn.commit()
        pg_cursor.close()
        pg_conn.close()

        # MySQL: Đơn hàng đổi trạng thái
        my_conn = get_mysql_conn()
        my_cursor = my_conn.cursor()
        my_cursor.execute("UPDATE orders SET status = 'COMPLETED' WHERE id = %s", (order_id,))
        my_conn.commit()
        my_cursor.close()
        my_conn.close()

        print(f"[V] Xong! Đã chốt đơn ID: {order_id}")
        
        # RabbitMQ: Queue giảm đi 1 message
        ch.basic_ack(delivery_tag=method.delivery_tag)
        
    except Exception as e:
       print(f"[X] Lỗi xử lý: {e}")
       ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def main():
    print("[*] Đang chờ RabbitMQ khởi động...")

    while True:
        try:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(host='rabbitmq')
            )
            print("[✓] Đã kết nối RabbitMQ")
            break
        except pika.exceptions.AMQPConnectionError:
            print("[...] RabbitMQ chưa sẵn sàng, đợi 5s...")
            time.sleep(5)

    channel = connection.channel()
    channel.queue_declare(queue='order_queue', durable=True)
    channel.basic_consume(
        queue='order_queue',
        on_message_callback=process_order
    )

    print("[*] Worker đang thức 24/7 chờ đơn hàng từ RabbitMQ...")
    channel.start_consuming()

if __name__ == '__main__':
    main()