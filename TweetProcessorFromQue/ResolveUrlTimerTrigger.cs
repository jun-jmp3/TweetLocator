using System.IO;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Azure.WebJobs.Host;
using Newtonsoft.Json;
using System;
using Microsoft.WindowsAzure.Storage.Table;
using System.Collections.Generic;
using System.Net.Http;
using System.Net;
using System.Text;

namespace TweetProcessorFromQue
{
    public static class ResolveUrlTimerTrigger
    {
        [FunctionName("ResolveUrlTimerTrigger")]
        public static void Run(
            [TimerTrigger("0 */5 * * * *")]TimerInfo myTimer, 
            [Table("TweetLocation")] CloudTable inputTable,
            [Table("TweetLocation3")] CloudTable outputTable,
            TraceWriter log,
            ExecutionContext context)
        {
            log.Info($"C# Timer trigger function executed at: {DateTime.Now}");

            var querySegment = inputTable.ExecuteQuerySegmentedAsync(new TableQuery<TweetLocationTable>(), null);
            foreach (TweetLocationTable item in querySegment.Result)
            {
                log.Info($"Data loaded: '{item.PartitionKey}' | '{item.RowKey}' | '{item.ScreenName}' | '{item.Text}'");
            }

            log.Info("Done.");

        }
    }
}
