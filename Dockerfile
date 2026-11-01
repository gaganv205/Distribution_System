# One image, two roles: the API and the worker run the same code with different commands.
FROM python:3.12-slim
WORKDIR /srv
ENV PYTHONUNBUFFERED=1 MODEL_PATH=/srv/model/model.joblib
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app ./app
COPY scripts ./scripts
# Train at build time so every container ships the identical model artifact.
RUN python scripts/train_model.py
RUN useradd --create-home appuser
USER appuser
EXPOSE 8000
CMD ["uvicorn", "app.api:app", "--host", "0.0.0.0", "--port", "8000"]
