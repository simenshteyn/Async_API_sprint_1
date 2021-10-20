FROM python:3.9.7-buster
WORKDIR /sites
COPY requirements.txt /sites
RUN pip install -r /sites/requirements.txt --no-cache-dir
COPY . /sites
EXPOSE 8000
CMD ["gunicorn", "main:app", "--bind", "0.0.0.0:8000", "-k", "uvicorn.workers.UvicornWorker"]