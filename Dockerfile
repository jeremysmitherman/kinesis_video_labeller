FROM python:3

WORKDIR /usr/src/app
COPY . .
RUN pip install --no-cache-dir -r requirements.txt
ENV PYTHONPATH /usr/src/app
CMD [ "python", "kinesis_video_labeller"]
