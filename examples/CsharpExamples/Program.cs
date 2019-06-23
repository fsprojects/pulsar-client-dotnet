using Pulsar.Client.Api;
using System;
using System.Threading.Tasks;

namespace CsharpExamples
{
    class Program
    {
        static async Task Main(string[] args)
        {
            Console.WriteLine("Example started");

            var client =
                new PulsarClientBuilder()
                    .WithServiceUrl("pulsar://pulsar-broker:31002")
                    .Build();

            var producer =
                await new ProducerBuilder(client)
                    .Topic("my-topic")
                    .CreateAsync();
            var messageId = await producer.SendAndWaitAsync(new byte[0]);
            Console.WriteLine(messageId);

            var consumer =
                await new ConsumerBuilder(client)
                        .Topic("my-topic")
                        .SubscriptionName("my-subscription")
                        .SubscribeAsync();
            var message = await consumer.ReceiveAsync();
            Console.WriteLine(message);
            await consumer.AcknowledgeAsync(message);

            Console.WriteLine("Example ended");

        }
    }
}
