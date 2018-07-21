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
            [Table("TweetLocation", Connection = "AzureWebJobsStorage")] CloudTable inputTable,
            [Table("TweetLocation3", Connection = "AzureWebJobsStorage")] ICollector<TweetLocationTable> outputTable,
            TraceWriter log,
            ExecutionContext context)
        {
            log.Info($"C# Timer trigger function executed at: {DateTime.Now}");

            /*
            var querySegment = inputTable.ExecuteQuerySegmentedAsync(new TableQuery<TweetLocationTable>().Where(
                TableQuery.GenerateFilterCondition("ScreenName", QueryComparisons.Equal, "fjun2347")), null);
                */
            var querySegment = inputTable.ExecuteQuerySegmentedAsync(new TableQuery<TweetLocationTable>(), null);
            foreach (TweetLocationTable item in querySegment.Result)
            {
                try
                {
                    log.Info($"Data loaded: '{item.PartitionKey}' | '{item.RowKey}' | '{item.ScreenName}' | '{item.Url}'");
                    string realUrl = "";
                    if (!string.IsNullOrEmpty(item.Url))
                    {

                        try
                        {

                            WebRequest req = WebRequest.Create(item.Url);

                            req.GetResponseAsync().ContinueWith(task =>
                            {
                                using (WebResponse res = task.Result)
                                {
                                    realUrl = res.ResponseUri.AbsoluteUri;
                                }
                            });

                        }
                        catch (Exception ex)
                        {
                            log.Error($"Exception: {ex.Message},{ex.StackTrace}");

                            if (ex.InnerException != null)
                            {
                                log.Error($"InnerException:  {ex.InnerException.Message}, {ex.InnerException.StackTrace}");
                            }
                        }
                    }

                    outputTable.Add(new TweetLocationTable()
                    {
                        PartitionKey = item.PartitionKey,
                        RowKey = item.RowKey,
                        TweetID = item.TweetID,
                        TweetTime = item.TweetTime,
                        UserID = item.UserID,
                        ScreenName = item.ScreenName,
                        Text = item.Text,
                        Url = realUrl,
                        Location = item.Location,
                        PlaceID = item.PlaceID,
                        Latitude = item.Latitude,
                        Longitude = item.Longitude
                    });

                } catch (Exception ex) {
                    log.Error($"Insert Error: {ex.Message}");
                }            
            }

            log.Info("Done.");

        }
    }
}
