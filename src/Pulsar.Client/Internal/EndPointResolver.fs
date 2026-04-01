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
        let addr = Volatile.Read(&addresses)
        let uri = addr[(index &&& Int32.MaxValue) % addr.Length]
        DnsEndPoint(uri.Host, uri.Port)
        
    member this.UpdateAddresses(newAddresses: Uri array) =
        if Array.isEmpty newAddresses then
            invalidArg "newAddresses" "Addresses list could not be empty."
        Volatile.Write(&addresses, newAddresses)
        Interlocked.Exchange(&currentIndex, -1) |> ignore
