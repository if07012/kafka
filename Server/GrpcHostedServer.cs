using Server;
using Server.Services;

internal class GrpcHostedServer : IHostedService
{
    private readonly ILogger<GreeterService> logger;
    Grpc.Core.Server _server = null;
    int port = 5223;
    public GrpcHostedServer(ILogger<GreeterService> logger)
    {
        this.logger = logger;
    }
    public Task StartAsync(CancellationToken cancellationToken)
    {
        _server = new Grpc.Core.Server()
        {
            Services = { Greeter.BindService(new Server.Services.GreeterService(logger)) },
            Ports = { new Grpc.Core.ServerPort("localhost", port, Grpc.Core.ServerCredentials.Insecure) }

        };
        Console.WriteLine("Start Grpc");
        _server.Start();
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        return _server.ShutdownAsync();
    }
}