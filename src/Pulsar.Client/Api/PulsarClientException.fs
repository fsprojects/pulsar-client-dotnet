namespace Pulsar.Client.Api

exception InvalidServiceURL
exception InvalidConfigurationException of string
exception NotFoundException of string
exception TimeoutException of string
exception IncompatibleSchemaException of string
exception LookupException of string
exception TooManyRequestsException of string
exception ConnectException of string
exception AlreadyClosedException of string
exception TopicTerminatedException of string
exception AuthenticationException of string
exception AuthorizationException of string
exception GettingAuthenticationDataException of string
exception UnsupportedAuthenticationException of string
exception BrokerPersistenceException of string
exception BrokerMetadataException of string
exception ProducerBusyException of string
exception ConsumerBusyException of string
exception NotConnectedException of string
exception InvalidMessageException of string
exception InvalidTopicNameException of string
exception NotSupportedException of string
exception ProducerQueueIsFullError of string
exception ProducerBlockedQuotaExceededError of string
exception ProducerBlockedQuotaExceededException of string
exception ChecksumException of string
exception CryptoException of string
exception TopicDoesNotExistException of string

// custom exception
exception ConnectionFailedOnSend of string
exception MaxMessageSizeChanged of int
exception SchemaSerializationException of string

exception DecompressionException of string
exception BatchDeserializationException of string

module PulsarClientException =
    let isRetriableError ex =
        match ex with
        | TooManyRequestsException _
        | AuthorizationException _
        | InvalidServiceURL _
        | InvalidConfigurationException _
        | NotFoundException _
        | IncompatibleSchemaException _
        | TopicDoesNotExistException _
        | UnsupportedAuthenticationException _
        | InvalidMessageException _
        | InvalidTopicNameException _
        | NotSupportedException _
        | ChecksumException _
        | CryptoException _
        | ProducerBusyException _
        | ConsumerBusyException _
             -> false
        | _ -> true

