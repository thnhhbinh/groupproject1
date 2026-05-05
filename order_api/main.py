from fastapi import FastAPI, HTTPException
import pika, json, mysql.connector
import redis

app = FastAPI()

redis_client = redis.Redis(host='redis', port=6379, db=0, decode_responses=True)

def get_mysql_conn():
    return mysql.connector.connect(
        host='mysql-db', user='root', password='root', database='noah_store'
    )

@app.post("/orders")
def create_order(order: dict):
    if order.get("quantity", 0) <= 0:
        raise HTTPException(status_code=400, detail="Số lượng phải > 0")

    product_id = order.get('product_id', 1)
    quantity = order['quantity']

    try:
        new_stock = redis_client.decrby(f"product:{product_id}:stock", quantity)
        if new_stock < 0:
            redis_client.incrby(f"product:{product_id}:stock", quantity)
            raise HTTPException(status_code=400, detail="Out of Stock")
    except redis.exceptions.RedisError as e:
        raise HTTPException(status_code=500, detail=f"Redis error: {e}")
    except HTTPException:
        raise

    # Lưu MySQL trạng thái PENDING
    try:
        conn = get_mysql_conn()
        cursor = conn.cursor()

        product_id = order.get('product_id', 1)
        cursor.execute("SELECT price FROM products WHERE id = %s", (product_id,))
        product = cursor.fetchone()
        if not product:
            raise HTTPException(status_code=404, detail="Product not found")
        price = product[0]
        total_price = float(price) * order['quantity']

        cursor.execute(
            "INSERT INTO orders (user_id, product_id, quantity, total_price, status) VALUES (%s, %s, %s, %s, 'PENDING')",
            (order.get('user_id', 1), product_id, order['quantity'], total_price)
        )
        order_id = cursor.lastrowid
        conn.commit()
    finally:
        if cursor:
           cursor.close()
        if conn:   
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

    try:
        redis_client.publish("new_orders", json.dumps({"order_id": order_id, "status": "PENDING"}))
    except Exception as e:
        print(f"Failed to publish to redis: {e}")

    # HTTP Response 200 OK chuẩn đặc tả
    return {"message": "Order received", "order_id": order_id}
@app.get("/")
def root():
    return {"message": "Order API running"}    