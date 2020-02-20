namespace Pulsar.Client.Internal

open System
open System.Net
open System.Threading

type internal EndPointResolver(addresses : Uri list) =
    let mutable currentIndex = -1
    
    do
        if List.isEmpty addresses then
            invalidArg "addresses" "Addresses list could not be empty."
            
    member this.Resolve() =
        let index = Interlocked.Increment(&currentIndex)
        let uri = addresses.[index % addresses.Length]
        DnsEndPoint(uri.Host, uri.Port)