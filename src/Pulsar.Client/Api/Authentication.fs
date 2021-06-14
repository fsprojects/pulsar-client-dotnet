namespace Pulsar.Client.Api

open System

[<AbstractClass>]
type Authentication() =
    interface IDisposable with
          member this.Dispose() = ()
            
    /// Return the identifier for this authentication method
    abstract member GetAuthMethodName: unit -> string

    /// Return The authentication data identifying this client that will be sent to the broker
    abstract member GetAuthData: unit -> AuthenticationDataProvider

    /// Get/Create an authentication data provider which provides the data that this client will be sent to the broker.
    abstract member GetAuthData: string -> AuthenticationDataProvider
    default this.GetAuthData brokerHostName =
        this.GetAuthData()

    
    static member AuthenticationDisabled =
        {
            new Authentication() with
                member this.GetAuthMethodName() = "none"
                member this.GetAuthData() = AuthenticationDataProvider()
        }