using System;
using System.Text;
using System.Threading.Tasks;
using Pulsar.Client.Api;
using System.Security.Cryptography.X509Certificates;

namespace CsharpExamples
{
    internal class TlsAuthentication
    {
        internal static async Task RunTlsAuthentication()
        {
            const string serviceUrl = "pulsar+ssl://my-pulsar-cluster:6651";
            const string subscriptionName = "my-subscription";
            var topicName = $"my-topic-{DateTime.Now.Ticks}";
            var ca = new X509Certificate2(@"path-to-ca.crt");
            var userTls = AuthenticationFactory.Tls(@"path-to-user.pfx");
                
            var client = await new PulsarClientBuilder()
                .ServiceUrl(serviceUrl)
                .EnableTls(true)
                .TlsTrustCertificate(ca)
                .Authentication(userTls)
                .BuildAsync();

            var producer = await client.NewProducer()
                .Topic(topicName)
                .CreateAsync();

            var consumer = await client.NewConsumer()
                .Topic(topicName)
                .SubscriptionName(subscriptionName)
                .SubscribeAsync();

            var messageId = await producer.SendAsync(Encoding.UTF8.GetBytes($"Sent from C# at '{DateTime.Now}'"));
            Console.WriteLine($"MessageId is: '{messageId}'");

            var message = await consumer.ReceiveAsync();
            Console.WriteLine($"Received: {Encoding.UTF8.GetString(message.Data)}");

            await consumer.AcknowledgeAsync(message.MessageId);
        }
    }
}