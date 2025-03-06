namespace Pulsar.Client.Auth    

open System.Collections.Generic
open Pulsar.Client.Api

type internal AuthenticationDataToken (supplier: unit -> string) =
    inherit AuthenticationDataProvider()

    override this.HasDataFromCommand() =
        true

    override this.GetCommandData() =
        supplier()

    override this.HasDataForHttp()=
        true

    //  Since AuthenticationOauth2 and AuthenticationToken both return this class when call GetAuthData()
    //  We only need to realize this DataToken class GetHttpHeaders() to realize http authentication
    override this.GetHttpHeaders()=
        let httpHeaderPropertiesDict = Dictionary<string, string>()
        httpHeaderPropertiesDict.Add("X-Pulsar-Auth-Method-Name", "token")
        httpHeaderPropertiesDict.Add("Authorization", "Bearer " + supplier())
        httpHeaderPropertiesDict



