using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Pulsar.Client.Api;

namespace OldDotnetExample
{
    internal class Program
    {
        internal static async Task Main(string[] args)
        {
            var loggerFactory =
                LoggerFactory.Create(builder =>
                    builder
                        .SetMinimumLevel(LogLevel.Information)
                        .AddConsole()
                );
            PulsarClient.Logger = loggerFactory.CreateLogger("PulsarLogger");

            await Simple.RunSimple();
            //await TlsAuthentication.RunTlsAuthentication();

            PulsarClient.Logger.LogInformation("Example ended. Press any key to exit");
            Console.ReadKey();
        }
    }
}