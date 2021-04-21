namespace Pulsar.Client.Common

open Microsoft.Extensions.Logging
open Microsoft.Extensions.Logging.Abstractions

type internal Log() =
    static let mutable _logger: ILogger = (NullLogger.Instance :> ILogger)

    static member Logger
        with get() = _logger
        and set(value) = _logger <- value