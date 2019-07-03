using Pulsar.Client.Api;
using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Console;
using Microsoft.Extensions.Logging;
using System.Text;

namespace CsharpExamples
{
    class Program
    {
        static async Task Main(string[] args)
        {
            Console.WriteLine("Example started");

            PulsarClient.Logger = new ConsoleLogger("PulsarLogger", (x, y) => true, true);

            var client =
                new PulsarClientBuilder()
                    .WithServiceUrl("pulsar://pulsar-broker:31002")
                    .Build();

            var producer =
                await new ProducerBuilder(client)
                    .Topic("my-topic")
                    .CreateAsync();
            var messageId = await producer.SendAndWaitAsync(Encoding.UTF8.GetBytes("Sent from C#"));
            Console.WriteLine(messageId);

            var consumer =
                await new ConsumerBuilder(client)
                        .Topic("my-topic")
                        .SubscriptionName("my-subscription")
                        .SubscribeAsync();
            var message = await consumer.ReceiveAsync();
            Console.WriteLine("Received: " + Encoding.UTF8.GetString(message.Payload));
            await consumer.AcknowledgeAsync(message);

            Console.WriteLine("Example ended");

        }
    }
}
