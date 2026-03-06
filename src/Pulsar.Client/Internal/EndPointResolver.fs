namespace Pulsar.Client.Internal

open System
open System.Net
open System.Threading

type internal EndPointResolver(initialAddresses : Uri list) =
    let mutable currentIndex = -1
    let mutable addresses = initialAddresses
    
    do
        if List.isEmpty addresses then
            invalidArg "addresses" "Addresses list could not be empty."
            
    member this.Resolve() =
        let index = Interlocked.Increment(&currentIndex)
        let addr = addresses
        let uri = addr.[Math.Abs(index) % addr.Length]
        DnsEndPoint(uri.Host, uri.Port)
        
    member this.UpdateAddresses(newAddresses: Uri list) =
        if List.isEmpty newAddresses then
            invalidArg "addresses" "Addresses list could not be empty."
        addresses <- newAddresses