namespace Pulsar.Client.Internal

open System.Collections
open FSharp.UMX
open Pulsar.Client.Common

type BatchMessageAcker(batchSize: int) =
    let bitSet = BitArray(batchSize, true)
    let mutable unackedCount = batchSize

    member this.AckIndividual (batchIndex: BatchIndex) =
        let previous = bitSet.[%batchIndex]
        if previous then
            bitSet.Set(%batchIndex, false)
            unackedCount <- unackedCount - 1
        unackedCount = 0

    member this.AckGroup (batchIndex: BatchIndex) =
        for i in 0 .. %batchIndex do
            if bitSet.[i] then
                bitSet.[i] <- false
                unackedCount <- unackedCount - 1
        unackedCount = 0

    // debug purpose
    member this.GetOutstandingAcks() =
        unackedCount

    member this.GetBatchSize() =
        batchSize

    member val PrevBatchCumulativelyAcked = false with get, set

    // Stub for batches that don't need acker at all
    static member NullAcker =
        Unchecked.defaultof<BatchMessageAcker>

    override this.Equals acker =  true

    override this.GetHashCode () = 0

    override this.ToString() = "UnackedCount: " + unackedCount.ToString()

    interface System.IComparable with
         member x.CompareTo yobj = 0