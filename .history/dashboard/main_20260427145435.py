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

    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "orders": orders
        }
    )