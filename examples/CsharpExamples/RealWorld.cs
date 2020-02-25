using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Pulsar.Client.Api;
using Pulsar.Client.Common;

namespace CsharpExamples
{
    internal class RealWorld
    {
        internal static async Task SendMessage(IProducer producer, ILogger logger, byte[] message)
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
        
        internal static async Task ProcessMessages(IConsumer consumer, ILogger logger, Func<Message, Task> f,
            CancellationToken ct)
        {
            try
            {
                while (!ct.IsCancellationRequested)
                {
                    var success = false;
                    var message = await consumer.ReceiveAsync();
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

            var client = new PulsarClientBuilder()
                .ServiceUrl(serviceUrl)
                .Build();

            var producer = await new ProducerBuilder(client)
                .Topic(topicName)
                .EnableBatching(false)
                .CreateAsync();

            var consumer = await new ConsumerBuilder(client)
                .Topic(topicName)
                .SubscriptionName(subscriptionName)
                .SubscribeAsync();

            var cts = new CancellationTokenSource();
            Task.Run(() => ProcessMessages(consumer, logger, (message) =>
            {
                var messageText = Encoding.UTF8.GetString(message.Data);
                logger.LogInformation("Received: {0}", messageText);
                return Task.CompletedTask;
            }, cts.Token));

            for (int i = 0; i < 100; i++)
            {
                await SendMessage(producer, logger, Encoding.UTF8.GetBytes($"Sent from C# at {DateTime.Now} message #{i}"));
            }
            
            cts.Dispose();
            await Task.Delay(200);// wait for pending acknowledgments to complete
            await consumer.CloseAsync();
            await producer.CloseAsync();
            await client.CloseAsync();
        }
    }
}