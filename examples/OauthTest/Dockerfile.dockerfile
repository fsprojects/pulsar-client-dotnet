FROM apachepulsar/pulsar-standalone:2.7.1
WORKDIR /pulsar
#using workdir /pulsar we copy public key and it's path will be /pulsar/oauth_public.key and this value must be same as in properties for standalone
COPY oauth_public.key oauth_public.key
#note credentials_file.json is being copied from into workdir /pulsar which is linked in brokerClientAuthenticationParameters
COPY credentials_file.json credentials_file.json
COPY standalone.conf conf/standalone.conf