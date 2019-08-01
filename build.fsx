#r "paket: groupref Build //"
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
  !! "**/*.*proj"
    |> MSBuild.runRelease id buildDir "Build"
    |> Trace.logItems "AppBuild-Output: "
)

Target.create "Restore" (fun _ ->
   !! "**/*.*proj"
     |> Seq.iter (fun proj -> DotNet.restore id proj)
 )

Target.create "RunTests" (fun _ ->
    !!("tests/UnitTests/UnitTests.fsproj")
    |> Seq.iter (DotNet.test id)
)

Target.create "Default" (fun _ -> Trace.trace "Default target was run")

open Fake.Core.TargetOperators

"Clean"
  ==> "Restore"
  ==> "BuildApp"
  ==> "RunTests"
  ==> "Default"

// start build
Target.runOrDefault "Default"