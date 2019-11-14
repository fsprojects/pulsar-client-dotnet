namespace Pulsar.Client.Internal

open System
open Pulsar.Client.Common

type internal BackoffConfig =
    {
        Initial: TimeSpan
        Max: TimeSpan
        MandatoryStop: TimeSpan
    }
    static member Default =
        {
            Initial = TimeSpan.Zero
            Max = TimeSpan.Zero
            MandatoryStop = TimeSpan.Zero
        }

type internal Backoff (config: BackoffConfig) =

    let initial = config.Initial.TotalMilliseconds
    let max = config.Max.TotalMilliseconds
    let mandatoryStop = config.MandatoryStop.TotalMilliseconds
    let mutable next = initial
    let mutable mandatoryStopMade = false
    let mutable firstBackoffTime = DateTime.MinValue

    member this.Next() =
        let mutable current = next;
        if (current < max) then
            next <- Math.Min(next * 2.0, max)

        // Check for mandatory stop
        if not mandatoryStopMade then
            let now = DateTime.Now
            let mutable timeElapsedSinceFirstBackoff = 0.0;
            if initial = current then
                firstBackoffTime <- now
            else
                timeElapsedSinceFirstBackoff <- (now - firstBackoffTime).TotalMilliseconds

            if (timeElapsedSinceFirstBackoff + current > mandatoryStop) then
                current <- Math.Max(initial, mandatoryStop - timeElapsedSinceFirstBackoff)
                mandatoryStopMade <- true

        // Randomly decrease the timeout up to 10% to avoid simultaneous retries
        //// If current < 10ms then current/10 < 1 and we get an exception from Random saying "Bound must be positive"
        if (current > 10.0) then
            current <- current - RandomGenerator.NextDouble() * (current / 10.0)

        Math.Max(initial, current) |> int

    member this.ReduceToHalf() =
        if (next > initial) then
            next <- Math.Max(next / 2.0, initial)

    member this.Reset() =
        next <- initial
        mandatoryStopMade <- false