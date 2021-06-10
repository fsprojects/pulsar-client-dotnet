FROM apachepulsar/pulsar-standalone:2.7.1

WORKDIR /pulsar
#using workdir /pulsar we copy public key and it's path will be /pulsar/oauth_public.key and this value must be same as in properties for standalone
COPY oauth_public2.key oauth_public2.key
COPY client.conf conf/client.conf
#note credentials_file.json is being copied from into workdir /pulsar which is linked in brokerClientAuthenticationParameters
COPY credentials_file_server.json credentials_file_server.json
COPY standalone.conf conf/standalone.conf
ENV SERVICE_URL http://127.0.0.1:8080
EXPOSE 80:80 8080:8080 6650:6650

CMD [ "/pulsar/bin/pulsar", "standalone"]