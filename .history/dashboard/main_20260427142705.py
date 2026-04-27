from fastapi import FastAPI, Request
from fastapi.templating import Jinja2Templates
import requests

app = FastAPI()
templates = Jinja2Templates(directory="templates")


@app.get("/")
def dashboard(request: Request):
    try:
        orders = requests.get("http://order-api:8000/orders").json()
    except:
        orders = []

    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "orders": orders
        }
    )