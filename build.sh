mono .paket/paket.bootstrapper.exe
mono .paket/paket.exe install
dotnet tool install fake-cli --tool-path .fake
.fake/fake run build.fsx