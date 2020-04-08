using System;
using System.Text;
using System.Threading.Tasks;
using Pulsar.Client.Api;
using System.Security.Cryptography.X509Certificates;
using System.Security.Authentication;

namespace OldDotnetExample
{
    internal class TlsAuthentication
    {
        internal static async Task RunTlsAuthentication()
        {
            const string serviceUrl = "pulsar+ssl://my-pulsar-cluster:6651";
            const string subscriptionName = "my-subscription";
            var topicName = $"my-topic-{DateTime.Now.Ticks}";
            var ca = new X509Certificate2(@"path-to-ca.crt");
            var userTls = AuthenticationFactory.tls(@"path-to-user.pfx");
                
            var client = new PulsarClientBuilder()
                .ServiceUrl(serviceUrl)
                .EnableTls(true)
                .TlsProtocols(SslProtocols.Tls12 | SslProtocols.Tls11)  // for .NET Framework 4.6.*
                .TlsTrustCertificate(ca)
                .Authentication(userTls)
                .Build();

            var producer = await new ProducerBuilder(client)
                .Topic(topicName)
                .CreateAsync();

            var consumer = await new ConsumerBuilder(client)
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