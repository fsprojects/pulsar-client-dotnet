namespace Pulsar.Client.Internal

open System
open System.Net
open System.Threading

type internal EndPointResolver(initialAddresses : Uri array) =
    let mutable currentIndex = -1
    let mutable addresses = initialAddresses
    
    do
        if Array.isEmpty initialAddresses then
            invalidArg "initialAddresses" "Addresses list could not be empty."
            
    member this.Resolve() =
        let index = Interlocked.Increment(&currentIndex)
        let addr = addresses
        let uri = addr[(index &&& Int32.MaxValue) % addr.Length]
        DnsEndPoint(uri.Host, uri.Port)
        
    member this.UpdateAddresses(newAddresses: Uri array) =
        if Array.isEmpty newAddresses then
            invalidArg "newAddresses" "Addresses list could not be empty."
        addresses <- newAddresses
        currentIndex <- -1