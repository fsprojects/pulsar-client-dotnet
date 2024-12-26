Composer file launches four image dockers `apachepulsar/pulsar-test-latest-version:latest`:

##### standalone
Runs pulsar in standalone mode\
Forwards port 6650

##### init
Waits for the `standalone` image to run, 
then execute `./scripts/init-standalone.sh` for pulsar tuning

##### standaloneTls
Updates the file `conf/standalone.conf` to run the pulsar in standelone mode with tls support\
Runs pulsar in standalone mode without `functions worker`\
Forwards port 6651

The pulsar in standalone mode with tls support and with `functions worker` 
can't run correctly because of a line error:\
https://github.com/apache/pulsar/blob/v2.5.0/pulsar-broker/src/main/java/org/apache/pulsar/PulsarStandalone.java#L323  
in later versions has been fixed, but the docker image has a bug.
For this reason, the pulsar runs without `functions worker`, 
but all necessary settings are made in the `./scripts/init-standaloneTls.sh`

##### initTls
Waits for the `standaloneTls` image to run, 
then execute `./scripts/init-standaloneTls.sh` for pulsar tuning
