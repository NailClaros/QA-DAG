FROM apache/airflow:3.0.5

# Copy requirements file into image
COPY requirements.txt .

# Install only your extra deps
RUN pip install --no-cache-dir -r requirements.txt
