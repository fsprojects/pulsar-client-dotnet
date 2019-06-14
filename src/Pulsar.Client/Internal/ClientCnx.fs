namespace Pulsar.Client.Internal

open Pipelines.Sockets.Unofficial
open Pulsar.Client.Common

type ClientCnx =
    {
        Connection: SocketConnection
        ProducerId: ProducerId
        ConsumerId: ConsumerId
    }

