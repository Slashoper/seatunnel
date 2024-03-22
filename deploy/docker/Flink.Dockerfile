FROM flink:1.16.3-scala_2.12-java8

ARG SERVER_HOST=10.2.19.48
ARG SEATUNNEL_VERSION=2.3.5

RUN mkdir /opt/flink/project
RUN mkdir /opt/seatunnel

RUN cd /opt/flink/project \
    && wget http://${SERVER_HOST}/bdp/seatunnel/2.3.3/seatunnel/starter/seatunnel-flink-15-starter.jar

RUN cd /opt/flink/conf \
    && wget http://${SERVER_HOST}/bdp/seatunnel/fake_to_console.conf

RUN cd /opt \
    && wget http://${SERVER_HOST}/bdp/seatunnel/${SEATUNNEL_VERSION}/apache-seatunnel-${SEATUNNEL_VERSION}-bin.tar.gz   \
        && tar -zxf apache-seatunnel-${SEATUNNEL_VERSION}-bin.tar.gz --strip-components 1 -C ./seatunnel  \
        && rm -rf apache-seatunnel-${SEATUNNEL_VERSION}-bin.tar.gz
