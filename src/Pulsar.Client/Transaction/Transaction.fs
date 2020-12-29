namespace Pulsar.Client.Transaction

open System
open Pulsar.Client.Common

type TxnId = {
    MostSigBits: int64
    LeastSigBits: int64
}

type TxnOperations() = class end

[<AllowNullLiteral>]
type Transaction internal (timeout: TimeSpan, txnOperations: TxnOperations, txnId: TxnId) =
    
    member this.Id = txnId
    
    member this.Commit() =
        ()
        
    member this.Abort() =
        ()