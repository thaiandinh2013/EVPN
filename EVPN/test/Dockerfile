#FROM python:3.6.1-alpine
#RUN apk update \
#  && apk add \
#    build-base \
#    postgresql \
#    postgresql-dev \
#    libpq
#RUN mkdir /usr/src/app
#WORKDIR /usr/src/app
#COPY ./requirements.txt .
#RUN pip3 install -r requirements.txt
#ENV PYTHONUNBUFFERED 1
#COPY . .


FROM python:3
ADD requirements.txt /
ADD DataQualityCheck.py /
ADD MinIO_to_Mysql.py /

RUN pip3 install -r requirements.txt

CMD ["python3","MinIO_to_Mysql.py"]
COPY . .


