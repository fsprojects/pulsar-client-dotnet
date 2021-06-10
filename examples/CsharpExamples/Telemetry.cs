using System;
using System.Text;
using System.Threading.Tasks;
using OpenTelemetry;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using Pulsar.Client.Api;
using Pulsar.Client.Otel;

namespace CsharpExamples
{
    internal class Telemetry
    {
        internal static async Task RunTelemetry()
        {
            const string serviceUrl = "pulsar://my-pulsar-cluster:32268";
            const string subscriptionName = "my-subscription";
            const string producerSourceName = "pulsar.producer";
            const string consumerSourceName = "pulsar.consumer";
            
            var topicName = $"my-topic-{DateTime.Now.Ticks}";

            Sdk.CreateTracerProviderBuilder()
                .AddSource(producerSourceName, consumerSourceName)
                .SetResourceBuilder(ResourceBuilder.CreateDefault().AddService("telemetry"))
                .AddConsoleExporter().Build();
            
            using var prodIntercept = 
                new OTelProducerInterceptor.OTelProducerInterceptor<byte[]>(producerSourceName, PulsarClient.Logger);
            using var consIntercept =
                new OtelConsumerInterceptor.OTelConsumerInterceptor<byte[]>(consumerSourceName, PulsarClient.Logger);

            var client = await new PulsarClientBuilder()
                .ServiceUrl(serviceUrl)
                .BuildAsync();

            var producer = await client.NewProducer()
                .Intercept(prodIntercept)
                .Topic(topicName)
                .CreateAsync();

            var consumer = await client.NewConsumer()
                .Intercept(consIntercept)
                .Topic(topicName)
                .SubscriptionName(subscriptionName)
                .SubscribeAsync();

            var msgIdss = await Task.WhenAll(new[]
            {
                producer.SendAsync(Encoding.UTF8.GetBytes($"Sent 1 from C# at '{DateTime.Now}'")),
                producer.SendAsync(Encoding.UTF8.GetBytes($"Sent 2 from C# at '{DateTime.Now}'"))
            });
            
            var msgs = await Task.WhenAll(new[]
            {
                consumer.ReceiveAsync(),
                consumer.ReceiveAsync()
            });

            foreach (var msg in msgs)
            {
                Console.WriteLine(Encoding.UTF8.GetString(msg.GetValue()));
            }

            await consumer.AcknowledgeCumulativeAsync(msgs[1].MessageId);
        }
    }
}