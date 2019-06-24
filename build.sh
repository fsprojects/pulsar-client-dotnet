dotnet tool install paket -g
paket install
dotnet tool install fake-cli --tool-path .fake
.fake/fake run build.fsx
