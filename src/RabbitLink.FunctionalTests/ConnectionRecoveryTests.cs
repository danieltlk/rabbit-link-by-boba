#region Usings

using System;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using RabbitLink.Messaging;
using RabbitLink.Topology;
using Xunit;

#endregion

namespace RabbitLink.FunctionalTests
{
    public class ConnectionRecoveryTests
    {
        private static readonly TimeSpan PublishTimeout = TimeSpan.FromSeconds(10);

        [Fact]
        public async Task PublishConfirms_AfterBrokerClosedConnection()
        {
            var exchangeName = TestsOptions.TestExchangeName;
            var queueName = TestsOptions.TestQueueName;
            var connectionName = $"link.test.{Guid.NewGuid():D}";

            using var link = TestsOptions.GetLinkBuilder()
                .ConnectionName(connectionName)
                .RecoveryInterval(TimeSpan.FromSeconds(1))
                .Build();

            using var producer = link.Producer
                .Exchange(async cfg =>
                {
                    var ex = await cfg.ExchangeDeclare(exchangeName, LinkExchangeType.Fanout, autoDelete: true);
                    var q = await cfg.QueueDeclare(queueName, false, false, true, expires: TimeSpan.FromMinutes(1));

                    await cfg.Bind(q, ex);

                    return ex;
                })
                .ConfirmsMode(true)
                .PublishTimeout(PublishTimeout)
                .Build();

            await producer.PublishAsync(new LinkPublishMessage<byte[]>(new byte[0]));

            await CloseConnectionAsync(connectionName);

            // before the fix the producer stayed on the closed connection and this publish hung until timeout
            await producer.PublishAsync(new LinkPublishMessage<byte[]>(new byte[0]));
        }

        private static async Task CloseConnectionAsync(string connectionName)
        {
            using var http = new HttpClient { BaseAddress = new Uri(TestsOptions.ManagementUrl) };
            http.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue(
                "Basic",
                Convert.ToBase64String(Encoding.ASCII.GetBytes(TestsOptions.ManagementCredentials))
            );

            // management api shows a new connection with a delay
            for (var attempt = 0; attempt < 20; attempt++)
            {
                var json = await http.GetStringAsync("api/connections");
                var name = JsonDocument.Parse(json).RootElement
                    .EnumerateArray()
                    .Where(c => c.TryGetProperty("client_properties", out var props)
                                && props.TryGetProperty("connection_name", out var value)
                                && value.GetString() == connectionName)
                    .Select(c => c.GetProperty("name").GetString())
                    .FirstOrDefault();

                if (name != null)
                {
                    var response = await http.DeleteAsync($"api/connections/{Uri.EscapeDataString(name)}");
                    response.EnsureSuccessStatusCode();
                    return;
                }

                await Task.Delay(500);
            }

            throw new InvalidOperationException($"Connection {connectionName} not found in management api");
        }
    }
}
