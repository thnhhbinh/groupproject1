from fastapi import FastAPI, HTTPException
import pika, json, mysql.connector

app = FastAPI()

# Cấu hình kết nối MySQL (Hệ thống Bán hàng)
def get_mysql_conn():
    return mysql.connector.connect(
        host='mysql-db',
        user='root',
        password='root_password',
        database='web_store'
    )

@app.post("/api/orders")
async def create_order(order: dict):
    # 1. Validate: Số lượng phải > 0 [cite: 58]
    if order.get("quantity", 0) <= 0:
        raise HTTPException(status_code=400, detail="Số lượng phải > 0")

    # 2. Ghi nhận vào MySQL trạng thái PENDING [cite: 59]
    try:
        conn = get_mysql_conn()
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO orders (user_id, product_id, quantity, status) VALUES (%s, %s, %s, 'PENDING')",
            (order['user_id'], order['product_id'], order['quantity'])
        )
        order_id = cursor.lastrowid
        conn.commit()
    finally:
        cursor.close()
        conn.close()

    # 3. Publish vào RabbitMQ (order_queue) [cite: 60]
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
    channel = connection.channel()
    channel.queue_declare(queue='order_queue', durable=True)
    
    order_payload = {**order, "order_id": order_id}
    channel.basic_publish(
        exchange='',
        routing_key='order_queue',
        body=json.dumps(order_payload),
        properties=pika.BasicProperties(delivery_mode=2) 
    )
    connection.close()

    # 4. Phản hồi nhanh cho Client [cite: 61, 63]
    return {"message": "Order received", "order_id": order_id}