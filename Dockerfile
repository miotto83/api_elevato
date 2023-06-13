FROM python:3.11-slim

# Keep the python output visible in the docker logs
ENV PYTHONUNBUFFERED=1

# Create and switch to /app directory
WORKDIR /app

# Install required packages
RUN apt-get update && \
    apt-get install -y build-essential libpq-dev unixodbc-dev libxml2

# Install requirements
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy the application files to the container
COPY ./app /app

EXPOSE 5000

# Run the application
CMD ["uvicorn", "main.main:app", "--host", "0.0.0.0", "--port", "5000", "--reload"]