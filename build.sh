dotnet tool install paket --tool-path .paket
paket install
sudo chmod +rwx -R ~/Projects/OSS/pulsar-client-dotnet
dotnet build Pulsar.Client.sln
