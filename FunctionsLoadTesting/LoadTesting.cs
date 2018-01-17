using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.WindowsAzure.Storage.Queue;
using Microsoft.WindowsAzure.Storage.Table;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace FunctionsLoadTesting
{

    
    public static class LoadTesting
    {
        const string DEVICE_ID_HEADER = "counter";

        private static string GetDeviceId(int i)
        {
            return $"{DEVICE_ID_HEADER}-{i.ToString()}"; // change to fit the spamer i.ToString("00"); is better. 
        }

        [FunctionName("ClientStart")]
        public static async Task<HttpResponseMessage> ClientStarter(
            [HttpTrigger] HttpRequestMessage req,
            [OrchestrationClient]DurableOrchestrationClient starter,
            TraceWriter log)
        {
            log.Info($"Start: {DateTime.UtcNow.ToLongDateString()}");
            dynamic eventData = await req.Content.ReadAsAsync<object>(); 
            var instanceId = await starter.StartNewAsync("ClientOrchestrator", eventData); // eventData is not needed. However, the interface requires.
            var result = JsonConvert.SerializeObject(new JObject {["instanceId"] = instanceId});

            return new HttpResponseMessage()
            {
                Content = new StringContent(result, System.Text.Encoding.UTF8, "application/text")
            };
        }

        [FunctionName("ClientStop")]
        public static async Task<HttpResponseMessage> ClientTerminator(
            [HttpTrigger] HttpRequestMessage req,
            [OrchestrationClient] DurableOrchestrationClient terminator,
            TraceWriter log)
        {
            var body = await req.Content.ReadAsStringAsync();
            var restored = JsonConvert.DeserializeObject<JObject>(body);
            var instanceId = restored["instanceId"].Value<string>();
            
            await terminator.TerminateAsync(instanceId, "Stop command fires.");
            log.Info($"Stopped InstanceId: {instanceId}");
            var result = JsonConvert.SerializeObject(new JObject { ["instanceId"] = $"stop {instanceId}"});
            return new HttpResponseMessage()
            {
                Content = new StringContent(result, System.Text.Encoding.UTF8, "application/text")
            };
        }

        [FunctionName("ClientStatus")]
        public static async Task<HttpResponseMessage> ClientStatus(
     [HttpTrigger] HttpRequestMessage req,
     [OrchestrationClient] DurableOrchestrationClient client,
     TraceWriter log)
        {
            var body = await req.Content.ReadAsStringAsync();
            var restored = JsonConvert.DeserializeObject<JObject>(body);
            var status = await client.GetStatusAsync(restored["instanceId"].ToString());


            var result = $"clients status. -> {JsonConvert.SerializeObject(status)}";
            return new HttpResponseMessage()
            {
                Content = new StringContent(result, System.Text.Encoding.UTF8, "application/text")
            };
        }

        [FunctionName("ClientOrchestrator")]
        public static async Task ClientOrchestrator(
            [OrchestrationTrigger] DurableOrchestrationContext context)
        {
            const int agentNumber = 10;
            var tasks = new Task[agentNumber];
            for (int i = 0; i < agentNumber; i++)
            {
                var deviceId = GetDeviceId(i);
                tasks[i] = context.CallActivityAsync("Client", deviceId);
            }

            await Task.WhenAll(tasks);
        }

        [FunctionName("Client")]
        public static async Task ClientExec(
            [ActivityTrigger] string deviceId, [Queue("que2", Connection = "connectionString")] CloudQueue queue, [Table("table", Connection = "connectionString")]CloudTable table, TraceWriter log)
        {
            const int pollingInterval = 1;

            log.Info($"{deviceId} has been started.");
            var TableList = new List<string>();
            try
            {
                while (true)
                {
                    // check if the cancellation queue is coming.

                    var list = await GetListAsync(deviceId, table);

                    //// Loop through the results, displaying information about the entity.
                    foreach (Message entity in list)
                    {
                        if (TableList.Contains(entity.RowKey))
                            continue;

                        var payloadObj = Payload.FromText(entity.Text);
                        payloadObj.InsertedIntoQ3 = DateTime.UtcNow;
                        payloadObj.InsertIntervalAll = payloadObj.InsertedIntoQ3 - payloadObj.InsertedIntoQ1;
                        payloadObj.InsertInterval2 = payloadObj.InsertedIntoQ3 - payloadObj.InsertedIntoQ2;

                        JObject returnObj = new JObject() {
                        { "PartitionKey", entity.PartitionKey },
                        { "RowKey", entity.RowKey },
                        { "Text", payloadObj.ToText() },
                    };
                        Print(entity, payloadObj);
                        TableList.Add(entity.RowKey);
                        await EnqueueAsync(queue, returnObj.ToString());
                    }

                    await Task.Delay(TimeSpan.FromSeconds(pollingInterval));
                }
            } catch (Exception e)
            {
                log.Error($"Client Error Happens. {e.Message}", e);
            } finally
            {
                log.Info($"{deviceId} has been finished.");
            }


        }

        public async static Task<List<Message>> GetListAsync(string partitionKey, CloudTable table)
        {
            //Query  
            var query = new TableQuery<Message>()
                                        .Where(TableQuery.GenerateFilterCondition("PartitionKey",
                                                QueryComparisons.Equal, partitionKey));

            var results = new List<Message>();
            TableContinuationToken continuationToken = null;
            do
            {
                TableQuerySegment<Message> queryResults =
                    await table.ExecuteQuerySegmentedAsync(query, continuationToken);

                continuationToken = queryResults.ContinuationToken;

                results.AddRange(queryResults.Results);

            } while (continuationToken != null);

            return results;
        }

        private async static Task EnqueueAsync(CloudQueue queue, string text)
        {
            // Create a message and add it to the queue.
            CloudQueueMessage message = new CloudQueueMessage(text);
            await queue.AddMessageAsync(message);
        }
        private static void Print(Message entity, Payload payload)
        {
            var mesage = $"{entity.PartitionKey}, {entity.RowKey}\t{payload.InsertIntervalAll}\t{payload.InsertInterval1}\t{payload.InsertInterval2}";
            Console.WriteLine(mesage);

        }



        [FunctionName("SpamStart")]
        public static async Task SpammerOrchestrator(
            [OrchestrationTrigger] DurableOrchestrationContext backupContext)
        {
            // 1000 spam apps started
            var tasks = new Task[1000];

            for (int i = 0; i < 1000; i++)
            {
                tasks[i] = backupContext.CallActivityAsync("Spammer", $"counter-{i}");
            }
            await Task.WhenAll(tasks);

        }

        [FunctionName("Spammer")]
        public static async Task SpammerExec(
            [ActivityTrigger] string guid, [Queue("que1", Connection = "connectionString")] CloudQueue queue, TraceWriter log)
        {
            const int mesagesCount = 2;
            await queue.CreateIfNotExistsAsync();

            for (int i = 0; i < mesagesCount; i++)
            {
                var message = CreateMessage(guid);
                await queue.AddMessageAsync(message);
            }
        }
        public static CloudQueueMessage CreateMessage(string partitionKey)
        {
            var payload = new Payload()
            {
                Name = partitionKey,
                InsertedIntoQ1 = DateTime.UtcNow,
                Data = Guid.NewGuid().ToString(),
            };

            var message = new Message()
            {
                PartitionKey = partitionKey,
                RowKey = Guid.NewGuid().ToString(),
                Text = payload.ToText(),
            };

            var messageText = JsonConvert.SerializeObject(message);
            Console.WriteLine($"{partitionKey} {payload.InsertedIntoQ1.ToLongTimeString()} {message.RowKey}");
            return new CloudQueueMessage(messageText);
        }
    }
}
