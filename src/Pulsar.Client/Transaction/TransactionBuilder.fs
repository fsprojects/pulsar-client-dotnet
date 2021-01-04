namespace Pulsar.Client.Transaction

open System.Threading.Tasks
open Pulsar.Client.Api
open FSharp.Control.Tasks.V2.ContextInsensitive
open Pulsar.Client.Internal
open Microsoft.Extensions.Logging

type TransactionBuilder internal (transactionClient: TransactionCoordinatorClient, config: TransactionConfiguration) =
    
    let txnOperations = {
        AddPublishPartitionToTxn = fun (txnId, topicName) ->
            transactionClient.AddPublishPartitionToTxnAsync(txnId, topicName)
        AddSubscriptionToTxn = fun (txnId, topicName, subscription) ->
            transactionClient.AddSubscriptionToTxnAsync(txnId, topicName, subscription)
    }
    
    internal new(transactionClient) =
        TransactionBuilder(transactionClient, TransactionConfiguration.Default)
    
    member private this.With(newConfig: TransactionConfiguration) =
        TransactionBuilder(transactionClient, newConfig)
    
    member this.TransactionTimeout transactionTimeout =
        { config with
            TxnTimeout = transactionTimeout }
        |> this.With
        
    member this.BuildAsync() =
        task {
            let! txnId = transactionClient.NewTransactionAsync(config.TxnRequestTimeout)
            Log.Logger.LogDebug("Success to new txn. txnID: {0}", txnId)
            return Transaction(config.TxnTimeout, txnOperations, txnId)
        }
        
        
            

