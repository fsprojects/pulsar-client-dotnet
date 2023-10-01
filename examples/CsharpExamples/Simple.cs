using System;
using System.Diagnostics;
using System.Text;
using System.Threading.Tasks;
using Pulsar.Client.Api;

namespace CsharpExamples
{
    internal class Simple
    {
        internal static async Task RunSimple()
        {
            const string serviceUrl = "pulsar://127.0.0.1:6650";
            const string subscriptionName = "my-subscription";
            var topicName = $"my-topic-{DateTime.Now.Ticks}";

            var client = await new PulsarClientBuilder()
                .ServiceUrl(serviceUrl)
                .BuildAsync();
            //
            // var producer = await client.NewProducer()
            //     .Topic(topicName)
            //     .CreateAsync();

            var consumer = await client.NewConsumer()
                .Topic(topicName)
                .SubscriptionName(subscriptionName)
                .SubscribeAsync();

            // var messageId = await producer.SendAsync(Encoding.UTF8.GetBytes($"Sent from C# at '{DateTime.Now}'"));
            // Console.WriteLine($"MessageId is: '{messageId}'");


            var n = 10000000;

            // warmup
            for (var i = 0; i < n / 10; i++)
            {
                var message = await consumer.ReceiveAsync();
                await consumer.AcknowledgeAsync(message.MessageId);
            }

            var sw = new Stopwatch();
            sw.Start();
            for (var i = 0; i < n; i++)
            {
                var message = await consumer.ReceiveAsync();
                await consumer.AcknowledgeAsync(message.MessageId);

                if (i % 100000 == 0)
                    Console.WriteLine($"Received {i} messages");
            }

            sw.Stop();
            Console.WriteLine(
                $"Received {n} messages in {sw.ElapsedMilliseconds}ms. Speed: {n / (sw.ElapsedMilliseconds)}K msg/s");

            // await consumer.AcknowledgeAsync(message.MessageId);
        }
    }
}