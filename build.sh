dotnet tool install paket --tool-path .paket
paket install
dotnet tool install fake-cli --tool-path .fake
.fake/fake run build.fsx
