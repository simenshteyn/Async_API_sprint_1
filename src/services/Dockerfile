FROM python:3.9.7-buster
WORKDIR /sites
COPY requirements.txt /sites
RUN pip install -r /sites/requirements.txt --no-cache-dir
COPY etl/ /sites
EXPOSE 8000
CMD python /sites/load_data.py