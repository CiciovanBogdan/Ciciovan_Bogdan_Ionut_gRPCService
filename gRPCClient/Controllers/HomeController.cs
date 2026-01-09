using Grpc.Core;
using Grpc.Net.Client;
using Microsoft.AspNetCore.Mvc;
using System.Threading;
using Ciciovan_Bogdan_Ionut_gRPCService;

namespace gRPCClient.Controllers
{
    public class HomeController : Controller
    {
        public IActionResult Index()
        {
            return View();
        }

        public async Task<IActionResult> Unary(int id = 1)
        {
            var channel = GrpcChannel.ForAddress("https://localhost:7047");
            var client = new Greeter.GreeterClient(channel);
            var reply = await client.SendStatusAsync(new SRequest { No = id });

            return View("ShowStatus", (object)ChangetoDictionary(reply));
        }

        public async Task<IActionResult> ServerStreaming(int count = 5)
        {
            var channel = GrpcChannel.ForAddress("https://localhost:7047");
            var client = new Greeter.GreeterClient(channel);
            Dictionary<string, string> statusDict = new Dictionary<string, string>();
            var cts = new CancellationTokenSource();
            cts.CancelAfter(TimeSpan.FromSeconds(count));

            using (var call = client.SendStatusSS(new SRequest { }, cancellationToken: cts.Token))
            {
                try
                {
                    await foreach (var message in call.ResponseStream.ReadAllAsync())
                    {
                        if (message.StatusInfo != null && message.StatusInfo.Count > 0)
                        {
                            var key = message.StatusInfo[0].Author + "_" + DateTime.Now.Ticks;
                            statusDict[key] = message.StatusInfo[0].Description;
                        }
                    }
                }
                catch (RpcException ex) when (ex.StatusCode == Grpc.Core.StatusCode.Cancelled)
                {
                    // Log Stream cancelled
                }
            }
            return View("ShowStatus", (object)statusDict);
        }

        public async Task<IActionResult> ClientStreaming(string ids = "3,2,4")
        {
            var channel = GrpcChannel.ForAddress("https://localhost:7047");
            var client = new Greeter.GreeterClient(channel);
            Dictionary<string, string> statusDict = new Dictionary<string, string>();

            int[] statuses = ids.Split(',').Select(int.Parse).ToArray();

            using (var call = client.SendStatusCS())
            {
                foreach (var sT in statuses)
                {
                    await call.RequestStream.WriteAsync(new SRequest { No = sT });
                }
                await call.RequestStream.CompleteAsync();
                SResponse sRes = await call.ResponseAsync;
                foreach (StatusInfo status in sRes.StatusInfo)
                    statusDict.Add(status.Author, status.Description);
            }
            return View("ShowStatus", (object)statusDict);
        }

        public async Task<IActionResult> BiDirectionalStreaming(string ids = "3,2,4")
        {
            var channel = GrpcChannel.ForAddress("https://localhost:7047");
            var client = new Greeter.GreeterClient(channel);
            Dictionary<string, string> statusDict = new Dictionary<string, string>();

            int[] statusNo = ids.Split(',').Select(int.Parse).ToArray();

            using (var call = client.SendStatusBD())
            {
                var responseReaderTask = Task.Run(async () =>
                {
                    while (await call.ResponseStream.MoveNext())
                    {
                        var response = call.ResponseStream.Current;
                        foreach (StatusInfo status in response.StatusInfo)
                            statusDict.Add(status.Author, status.Description);
                    }
                });

                foreach (var sT in statusNo)
                {
                    await call.RequestStream.WriteAsync(new SRequest { No = sT });
                }
                await call.RequestStream.CompleteAsync();
                await responseReaderTask;
            }
            return View("ShowStatus", (object)statusDict);
        }

        private Dictionary<string, string> ChangetoDictionary(SResponse response)
        {
            Dictionary<string, string> statusDict = new Dictionary<string, string>();
            foreach (StatusInfo status in response.StatusInfo)
                statusDict.Add(status.Author, status.Description);
            return statusDict;
        }
    }
}