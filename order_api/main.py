from fastapi import FastAPI, HTTPException
import pika, json, mysql.connector

app = FastAPI()

def get_mysql_conn():
    return mysql.connector.connect(
        host='mysql-db', user='root', password='root_password', database='web_store'
    )

@app.post("/api/orders")
async def create_order(order: dict):
    if order.get("quantity", 0) <= 0:
        raise HTTPException(status_code=400, detail="Số lượng phải > 0")

    # Lưu MySQL trạng thái PENDING
    try:
        conn = get_mysql_conn()
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO orders (user_id, product_id, quantity, status) VALUES (%s, %s, %s, 'PENDING')",
            (order.get('user_id', 1), order.get('product_id', 1), order['quantity'])
        )
        order_id = cursor.lastrowid
        conn.commit()
    finally:
        cursor.close()
        conn.close()

    # Đẩy vào RabbitMQ (Queue thêm 1 message)
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
    channel = connection.channel()
    channel.queue_declare(queue='order_queue', durable=True)
    
    order_payload = {**order, "order_id": order_id}
    channel.basic_publish(
        exchange='', routing_key='order_queue',
        body=json.dumps(order_payload),
        properties=pika.BasicProperties(delivery_mode=2) 
    )
    connection.close()

    # HTTP Response 200 OK chuẩn đặc tả
    return {"message": "Order received", "order_id": order_id}