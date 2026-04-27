from fastapi import FastAPI, Request
from fastapi.templating import Jinja2Templates

app = FastAPI()

templates = Jinja2Templates(directory="templates")


@app.get("/")
def dashboard(request: Request):
    orders = [
        {"order_id": 1, "customer": "User 1", "status": "Completed", "total": 250000},
        {"order_id": 2, "customer": "User 2", "status": "Pending", "total": 180000},
        {"order_id": 3, "customer": "User 3", "status": "Completed", "total": 320000},
    ]

    total_orders = len(orders)
    total_revenue = sum(order["total"] for order in orders)
    completed_orders = sum(1 for order in orders if order["status"] == "Completed")

    return templates.TemplateResponse(
        request=request,
        name="index.html",
        context={
            "orders": orders,
            "total_orders": total_orders,
            "total_revenue": total_revenue,
            "completed_orders": completed_orders,
        },
    )