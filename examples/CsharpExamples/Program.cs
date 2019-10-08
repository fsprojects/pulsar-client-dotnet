using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Pulsar.Client.Api;

namespace CsharpExamples
{
    internal class Program
    {
        private static async Task Main(string[] args)
        {
            var loggerFactory =
               LoggerFactory.Create(builder =>
                   builder
                       .SetMinimumLevel(LogLevel.Information)
                       .AddConsole()
               );
            PulsarClient.Logger = loggerFactory.CreateLogger("PulsarLogger");

            await Simple.RunSimple();
            await CustomProps.RunCustomProps();

            Console.WriteLine("Example ended. Press any key to exit");
            Console.ReadKey();
        }
    }
}