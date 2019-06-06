open Microsoft.Build.Logging.StructuredLogger
open Microsoft.Build.Tasks
open Microsoft.Build.Tasks
#r "paket:
nuget Fake.IO.FileSystem
nuget Fake.DotNet.MSBuild
nuget Fake.Core.Target"
#load "./.fake/build.fsx/intellisense.fsx"

open Fake.IO
open Fake.IO.Globbing.Operators //enables !! and globbing
open Fake.DotNet
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
    
    |> MSBuild.runRelease id buildDir "Build"
    |> Trace.logItems "AppBuild-Output: "
)

Target.create "Default" (fun _ ->
  Trace.trace "Hello World from FAKE"
)

open Fake.Core.TargetOperators

"Clean"
  ==> "BuildApp"
  ==> "Default"

// start build
Target.runOrDefault "Default"