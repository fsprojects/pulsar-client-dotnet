using System;
using System.ComponentModel.DataAnnotations;
using System.Threading.Tasks;
using Microsoft.FSharp.Core;
using Pulsar.Client.Api;

namespace CsharpExamples
{
    public class Product
    {
        [Required]
        public string Name { get; set; }
        public double Rating { get; set; }
        public override string ToString()
        {
            return $"Name: {Name} Rating: {Rating}";
        }
    }
    
    internal class SchemaExample
    {
        internal static async Task RunSchema()
        {
            const string serviceUrl = "pulsar://my-pulsar-cluster:30002";
            const string subscriptionName = "my-subscription";
            var topicName = $"my-topic-{DateTime.Now.Ticks}";

            var client = await new PulsarClientBuilder()
                .ServiceUrl(serviceUrl)
                .BuildAsync();

            var producer = await client.NewProducer(Schema.JSON<Product>())
                .Topic(topicName)
                .CreateAsync();

            var consumer = await client.NewConsumer(Schema.JSON<Product>())
                .Topic(topicName)
                .SubscriptionName(subscriptionName)
                .SubscribeAsync();

            var messageId = await producer.SendAsync(new Product{ Name = "IPhone", Rating = 1.1 });
            Console.WriteLine($"MessageId is: '{messageId}'");

            var message = await consumer.ReceiveAsync();
            Console.WriteLine($"Received: {message.GetValue()}");

            await consumer.AcknowledgeAsync(message.MessageId);
        }
    }
}