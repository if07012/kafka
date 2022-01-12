using Grpc.Core;
using Grpc.Net.Client;
using Server;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Client
{
    internal class Program
    {
        static async Task Main(string[] args)
        {
            while (true)
            {
                Thread.Sleep(1000);
                //using var channel = GrpcChannel.ForAddress("https://localhost:7178");
                var channel = new Channel("localhost:5223", Grpc.Core.ChannelCredentials.Insecure);
                var client = new Greeter.GreeterClient(channel);
                var reply = await client.SayHelloAsync(
                                  new HelloRequest { Name = "GreeterClient" });
                Console.WriteLine("Greeting: " + reply.Message);
            }

        }
    }
}
