using System;
using System.Text;
using System.Threading.Tasks;
using Pulsar.Client.Api;

namespace CsharpExamples
{
    internal class TransactionExample
    {
        internal static async Task RunTransaction()
        {
            const string serviceUrl = "pulsar://my-pulsar-cluster:31004";
            const string subscriptionName = "my-subscription";
            var topicName1 = $"my-topic-{DateTime.Now.Ticks}-1";
            var topicName2 = $"my-topic-{DateTime.Now.Ticks}-2";

            var client = await new PulsarClientBuilder()
                .ServiceUrl(serviceUrl)
                .EnableTransaction(true)
                .BuildAsync();

            var producer = await client.NewProducer(Schema.STRING())
                .Topic(topicName1)
                .CreateAsync();

            var consumer = await client.NewConsumer(Schema.STRING())
                .Topic(topicName2)
                .SubscriptionName(subscriptionName)
                .SubscribeAsync();
            
            var transProducer = await client.NewProducer(Schema.STRING())
                .Topic(topicName2)
                .SendTimeout(TimeSpan.Zero)
                .CreateAsync();

            var transConsumer = await client.NewConsumer(Schema.STRING())
                .Topic(topicName1)
                .SubscriptionName(subscriptionName)
                .SubscribeAsync();

            var message1 = producer.NewMessage("Sent from C# at " + DateTime.Now.ToString());
            var messageId1 = await producer.SendAsync(message1);
            Console.WriteLine("Message was initially sent with MessageId={0}", messageId1);

            var transaction =
                await client.NewTransaction()
                    .BuildAsync();

            var message2 = await transConsumer.ReceiveAsync();
            var text1 = message2.GetValue();
            Console.WriteLine("Message received in transaction: {0}", text1);
            await transConsumer.AcknowledgeAsync(message2.MessageId, transaction);

            var message3 = transProducer.NewMessage(text1, txn: transaction);
            var messageId3 = await transProducer.SendAsync(message3);
            Console.WriteLine("Message was produced in transaction, but not yet commited with MessageId={0}", messageId3);

            await transaction.Commit();

            var message4 = await consumer.ReceiveAsync();
            var text2 = message4.GetValue();
            Console.WriteLine("Message received normally: {0}", text2);
            await consumer.AcknowledgeAsync(message4.MessageId);
        }
    }
}