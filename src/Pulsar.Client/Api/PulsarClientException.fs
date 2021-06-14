namespace Pulsar.Client.Api

open System

type InvalidConfigurationException(msg) = inherit Exception(msg)
type NotFoundException(msg) = inherit Exception(msg)
type TimeoutException(msg) = inherit Exception(msg)
type IncompatibleSchemaException(msg) = inherit Exception(msg)
type LookupException(msg) = inherit Exception(msg)
type TooManyRequestsException(msg) = inherit Exception(msg)
type ConnectException(msg) = inherit Exception(msg)
type AlreadyClosedException(msg) = inherit Exception(msg)
type TopicTerminatedException(msg) = inherit Exception(msg)
type AuthenticationException(msg) = inherit Exception(msg)
type AuthorizationException(msg) = inherit Exception(msg)
type GettingAuthenticationDataException(msg) = inherit Exception(msg)
type UnsupportedAuthenticationException(msg) = inherit Exception(msg)
type BrokerPersistenceException(msg) = inherit Exception(msg)
type BrokerMetadataException(msg) = inherit Exception(msg)
type ProducerBusyException(msg) = inherit Exception(msg)
type ConsumerBusyException(msg) = inherit Exception(msg)
type NotConnectedException(msg) = inherit Exception(msg)
type InvalidMessageException(msg) = inherit Exception(msg)
type InvalidTopicNameException(msg) = inherit Exception(msg)
type NotSupportedException(msg) = inherit Exception(msg)
type ProducerQueueIsFullError(msg) = inherit Exception(msg)
type ProducerBlockedQuotaExceededError(msg) = inherit Exception(msg)
type ProducerBlockedQuotaExceededException(msg) = inherit Exception(msg)
type ChecksumException(msg) = inherit Exception(msg)
type CryptoException(msg) = inherit Exception(msg)
type TopicDoesNotExistException(msg) = inherit Exception(msg)
type ConsumerAssignException(msg) = inherit Exception(msg)
type NotAllowedException(msg) = inherit Exception(msg)
type UnsupportedVersionException(msg) = inherit Exception(msg)
type SubscriptionNotFoundException(msg) = inherit Exception(msg)
type ConsumerNotFoundException(msg) = inherit Exception(msg)
type MessageAcknowledgeException(msg) = inherit Exception(msg)
type TransactionConflictException(msg) = inherit Exception(msg)


// custom exception
type ConnectionFailedOnSend(msg) = inherit Exception(msg)
exception MaxMessageSizeChanged of int
type SchemaSerializationException(msg) = inherit Exception(msg)
type DecompressionException(msg) = inherit Exception(msg)
type BatchDeserializationException (msg) = inherit Exception(msg)

//transaction
type CoordinatorClientStateException (msg) = inherit Exception(msg)
type CoordinatorNotFoundException (msg) = inherit Exception(msg)
type InvalidTxnStatusException (msg) = inherit Exception(msg)
type TransactionNotFoundException (msg) = inherit Exception(msg)
type MetaStoreHandlerNotExistsException (msg) = inherit Exception(msg)
type MetaStoreHandlerNotReadyException (msg) = inherit Exception(msg)
type RequestTimeoutException (msg) = inherit Exception(msg)
type TokenExchangeException (msg) = inherit Exception(msg)


module PulsarClientException =
    let isRetriableError (ex: exn) =
        match ex with
        | :? AuthorizationException
        | :? InvalidConfigurationException
        | :? NotFoundException
        | :? IncompatibleSchemaException
        | :? TopicDoesNotExistException
        | :? UnsupportedAuthenticationException
        | :? InvalidMessageException
        | :? InvalidTopicNameException
        | :? NotSupportedException
        | :? NotAllowedException
        | :? ChecksumException
        | :? CryptoException
        | :? ConsumerAssignException
        | :? MessageAcknowledgeException
        | :? TransactionConflictException
        | :? ProducerBusyException
        | :? ConsumerBusyException
             -> false
        | _ -> true

