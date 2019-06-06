#r "paket:
nuget Fake.IO.FileSystem
nuget Fake.DotNet.MSBuild
nuget Fake.Core.Target
nuget Fake.DotNet.Cli
nuget Fake.DotNet.Paket"
#load "./.fake/build.fsx/intellisense.fsx"

open Fake.IO
open Fake.IO.Globbing.Operators //enables !! and globbing
open Fake.DotNet
open Fake.DotNet.NuGet.Restore
open Fake.Core

// Properties
let buildDir = "./build/"

// Targets
Target.create "Clean" (fun _ ->
  Shell.cleanDir buildDir
)

Target.create "BuildApp" (fun _ ->
  !! "src/*.fsproj"
    |> MSBuild.runRelease id buildDir "Build"
    |> Trace.logItems "AppBuild-Output: "
)

Target.create "Restore" (fun _ ->
  !! "src/*.fsproj"
    |> Seq.iter (fun proj -> DotNet.restore id proj)
)

Target.create "Default" (fun _ ->
  Trace.trace "Hello World from FAKE"
)

open Fake.Core.TargetOperators

"Clean"
  ==> "Restore"
  ==> "BuildApp"
  ==> "Default"

// start build
Target.run