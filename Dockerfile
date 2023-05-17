FROM python:latest
ENV dir_samples=/samples
ADD *.py .
ADD ./logs.old/* ./logs.old/
COPY requirements.txt requirements.txt
RUN pip install pytz
RUN pip install pandas
RUN pip install speedtest-cli
CMD ["python3", "speedcheckdaemon.py"]