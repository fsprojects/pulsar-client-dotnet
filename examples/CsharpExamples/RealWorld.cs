using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Pulsar.Client.Api;
using Pulsar.Client.Common;
#pragma warning disable 4014

namespace CsharpExamples
{
    internal class RealWorld
    {
        internal static async Task SendMessage(IProducer<byte[]> producer, ILogger logger, byte[] message)
        {
            try
            {
                var messageId = await producer.SendAsync(message);
                logger.LogInformation("MessageSent to {0}. MessageId={1}", producer.Topic, messageId);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Error on produce message to {0}", producer.Topic);
            }
        }

        internal static async Task ProcessMessages(IConsumer<byte[]> consumer, ILogger logger, Func<Message<byte[]>, Task> f,
            CancellationToken ct)
        {
            try
            {
                while (!ct.IsCancellationRequested)
                {
                    var success = false;
                    var message = await consumer.ReceiveAsync(ct);
                    try
                    {
                        await f(message);
                        success = true;
                        logger.LogDebug("Message handled");
                    }
                    catch (Exception e)
                    {
                        logger.LogError(e, "Can't process message {0}, MessageId={1}", consumer.Topic, message.MessageId);
                        await consumer.NegativeAcknowledge(message.MessageId);
                    }
                    if (success)
                    {
                        await consumer.AcknowledgeAsync(message.MessageId);
                        logger.LogDebug("Message acknowledged");
                    }
                }
            }
            catch (TaskCanceledException)
            {
                logger.LogInformation("ProcessMessages cancelled for {0}", consumer.Topic);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "ProcessMessages failed for {0}", consumer.Topic);
            }
        }


        internal static async Task RunRealWorld(ILogger logger)
        {
            const string serviceUrl = "pulsar://my-pulsar-cluster:30002";
            const string subscriptionName = "my-subscription";
            var topicName = $"my-topic-{DateTime.Now.Ticks}";

            var client = await new PulsarClientBuilder()
                .ServiceUrl(serviceUrl)
                .BuildAsync();

            var producer = await client.NewProducer()
                .Topic(topicName)
                .EnableBatching(false)
                .CreateAsync();

            var consumer = await client.NewConsumer()
                .Topic(topicName)
                .SubscriptionName(subscriptionName)
                .SubscribeAsync();

            using var cts = new CancellationTokenSource();
            var ctsToken = cts.Token;
            Task.Run(() =>
                ProcessMessages(consumer, logger, (message) =>
                {
                    var messageText = Encoding.UTF8.GetString(message.Data);
                    logger.LogInformation("Received: {0}", messageText);
                    return Task.CompletedTask;
                }, ctsToken),
                ctsToken);

            for (int i = 0; i < 100; i++)
            {
                await SendMessage(producer, logger, Encoding.UTF8.GetBytes($"Sent from C# at {DateTime.Now} message #{i}"));
            }

            cts.Cancel();
            await Task.Delay(200);// wait for pending acknowledgments to complete
            await consumer.DisposeAsync();
            await producer.DisposeAsync();
            await client.CloseAsync();
        }
    }
}