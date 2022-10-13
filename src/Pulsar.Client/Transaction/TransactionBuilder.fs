namespace Pulsar.Client.Transaction

open Pulsar.Client.Api

open Pulsar.Client.Common
open Microsoft.Extensions.Logging

type TransactionBuilder internal (transactionClient: TransactionCoordinatorClient, config: TransactionConfiguration) =

    let txnOperations = {
        AddPublishPartitionToTxn = fun (txnId, topicName) ->
            transactionClient.AddPublishPartitionToTxnAsync(txnId, topicName)
        AddSubscriptionToTxn = fun (txnId, topicName, subscription) ->
            transactionClient.AddSubscriptionToTxnAsync(txnId, topicName, subscription)
        Commit = fun txnId ->
            transactionClient.CommitAsync(txnId)
        Abort = fun txnId ->
            transactionClient.AbortAsync(txnId)
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
        backgroundTask {
            let! txnId = transactionClient.NewTransactionAsync(config.TxnTimeout)
            Log.Logger.LogDebug("Success to new txn. txnID: {0}", txnId)
            return Transaction(config.TxnTimeout, txnOperations, txnId)
        }




