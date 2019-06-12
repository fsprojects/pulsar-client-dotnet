namespace Pulsar.Client.Api

open System

module internal ExceptionHelper =

    let throwIf predicate createException =
        if predicate() then raise(createException())

    let invalidArgIf predicate message arg =
        throwIf
            (fun() -> predicate(arg))
            (fun() -> ArgumentException(message))
        arg

    let invalidArgIfBlankString message arg =
        invalidArgIf (String.IsNullOrWhiteSpace) message arg

    let throwIfBlankString createException arg =
        throwIf
            (fun() -> String.IsNullOrWhiteSpace(arg))
            createException
        arg