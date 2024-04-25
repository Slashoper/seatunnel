FROM flink:1.16.3-scala_2.12-java8

ARG SERVER_HOST=10.2.19.48
ARG SEATUNNEL_VERSION=2.3.5

USER root

#set the timezone
ENV TZ=Asia/Shanghai
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

RUN mkdir /opt/flink/project
# the seatunnel path should aggre with the seatunnel client
RUN mkdir -p /opt/beeline/seatunnel

#RUN cd /opt/flink/project \
#    && wget http://${SERVER_HOST}/bdp/seatunnel/2.3.3/seatunnel/starter/seatunnel-flink-15-starter.jar \

# add job config file into docker images does not work
#RUN cd /opt/flink/conf \
#    && wget http://${SERVER_HOST}/bdp/seatunnel/fake_to_console.conf
#COPY conf/flink-conf.yaml /opt/flink/conf

RUN cd /opt/beeline \
    && wget http://${SERVER_HOST}/bdp/seatunnel/${SEATUNNEL_VERSION}/apache-seatunnel-${SEATUNNEL_VERSION}-bin.tar.gz   \
        && tar -zxf apache-seatunnel-${SEATUNNEL_VERSION}-bin.tar.gz --strip-components 1 -C ./seatunnel  \
        && rm -rf apache-seatunnel-${SEATUNNEL_VERSION}-bin.tar.gz
USER flink