import pika, json, time, mysql.connector, psycopg2

def process_order(ch, method, properties, body):
    order_data = json.loads(body)
    print(f"[*] Đang xử lý đơn hàng: {order_data['order_id']}")

    # 1. Giả lập độ trễ xử lý thanh toán (1-2s) [cite: 70]
    time.sleep(2)

    try:
        # 2. Ghi vào PostgreSQL (Hệ thống Tài chính) [cite: 71]
        conn_pg = psycopg2.connect(host="postgres-db", database="finance", user="postgres", password="pg_password")
        cur_pg = conn_pg.cursor()
        cur_pg.execute(
            "INSERT INTO finance_transactions (order_id, user_id, amount) VALUES (%s, %s, %s)",
            (order_data['order_id'], order_data['user_id'], order_data['quantity'] * 100)
        )
        conn_pg.commit()

        # 3. Cập nhật trạng thái MySQL sang COMPLETED [cite: 72]
        conn_my = mysql.connector.connect(host="mysql-db", database="web_store", user="root", password="root_password")
        cur_my = conn_my.cursor()
        cur_my.execute("UPDATE orders SET status='COMPLETED' WHERE id=%s", (order_data['order_id'],))
        conn_my.commit()

        # 4. Gửi ACK (Xác nhận) để xóa tin nhắn khỏi hàng đợi [cite: 73]
        ch.basic_ack(delivery_tag=method.delivery_tag)
        print(f"[v] Đã hoàn thành đơn hàng {order_data['order_id']}")

    except Exception as e:
        print(f"[!] Lỗi: {e}")

# Kết nối RabbitMQ và lắng nghe order_queue [cite: 67, 69]
connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
channel = connection.channel()
channel.queue_declare(queue='order_queue', durable=True)
channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue='order_queue', on_message_callback=process_order)

print('[*] Đang chờ đơn hàng...')
channel.start_consuming()