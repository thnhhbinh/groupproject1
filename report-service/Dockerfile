FROM python:3.11

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY report_service.py .

CMD ["uvicorn", "report_service:app", "--host", "0.0.0.0", "--port", "7000"]