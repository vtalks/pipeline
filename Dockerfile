FROM alpine:latest
MAINTAINER Raul Perez <repejota@gmail.com>

ADD requirements.txt /tmp/requirements.txt

RUN apk update && \
  apk upgrade && \
  apk add --no-cache bash python3 git supervisor && \
  pip3 install -r /tmp/requirements.txt && \
  rm -rf /var/cache/apk/* && \
  mkdir -p /var/log/supervisor

ADD . /opt/pipeline
WORKDIR /opt/pipeline

CMD ["./entrypoint.sh"]