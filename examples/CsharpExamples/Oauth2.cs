using System;
using System.IO;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Pulsar.Client.Api;
using Pulsar.Client.Common;

namespace CsharpExamples
{
    internal class Oauth2
    {
        static string GetConfigFilePath()
        {
            var configFolderName = "Oauth2Files";
            var privateKeyFileName = "credentials_file.json";
            var startup = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);
            var indexOfConfigDir = startup.IndexOf("examples", StringComparison.Ordinal);
            var examplesFolder = startup.Substring(0, startup.Length - indexOfConfigDir - 3);
            var configFolder = Path.Combine(examplesFolder, configFolderName);
            var ret = Path.Combine(configFolder, privateKeyFileName);
            if (!File.Exists(ret)) throw new FileNotFoundException("can't find credentials file");
            return ret;
        }
//In order to run this example one has to have authentication on broker
//Check configuration files to see how to set up authentication in broker
//In this example Auth0 server is used, look at it's response in Auth0response file 
        internal static async Task RunOauth()
        {
            var fileUri = new Uri(GetConfigFilePath());
            var issuerUrl = new Uri("https://pulsar-sample.us.auth0.com");
            var audience = "https://pulsar-sample.us.auth0.com/api/v2/";

            var serviceUrl = "pulsar://localhost:6650";
            var subscriptionName = "my-subscription";
            var topicName =  $"my-topic-%{DateTime.Now.Ticks}";
            
            var client = await new PulsarClientBuilder()
                .ServiceUrl(serviceUrl)
                .Authentication(AuthenticationFactoryOAuth2.ClientCredentials(issuerUrl, audience, fileUri))
                .BuildAsync();
            
            var producer = await client.NewProducer()
                .Topic(topicName)
                .CreateAsync();

            var consumer = await client.NewConsumer()
                .Topic(topicName)
                .SubscriptionName(subscriptionName)
                .SubscriptionType(SubscriptionType.Exclusive)
                .SubscribeAsync();

            var messageId = await producer.SendAsync(Encoding.UTF8.GetBytes($"Sent from C# at '{DateTime.Now}'"));
            Console.WriteLine($"MessageId is: '{messageId}'");

            var message = await consumer.ReceiveAsync();
            Console.WriteLine($"Received: {Encoding.UTF8.GetString(message.Data)}");

            await consumer.AcknowledgeAsync(message.MessageId);
        }
    }
}