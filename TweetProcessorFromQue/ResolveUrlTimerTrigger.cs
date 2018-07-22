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

        private static Dictionary<string, string> urlCache = new Dictionary<string, string>();

        [FunctionName("ResolveUrlTimerTrigger")]
        public static void Run(
            [TimerTrigger("0 1 * * * *")]TimerInfo myTimer, 
            [Table("TweetLocation", Connection = "AzureWebJobsStorage")] CloudTable inputTable,
            [Table("TweetMaxID", Connection = "AzureWebJobsStorage")] CloudTable maxIDTable,
            [Table("TweetLocation3", Connection = "AzureWebJobsStorage")] ICollector<TweetLocationTable> outputTable,
            TraceWriter log,
            ExecutionContext context)
        {
            log.Info($"C# Timer trigger function executed at: {DateTime.Now}");


            // MAX IDを取得する。
            long maxTweetID = 0;
            try {

                var tableQuery = new TableQuery<TweetMaxIDTable>();

                var querySegment = maxIDTable.ExecuteQuerySegmentedAsync(tableQuery, null);
                foreach (TweetMaxIDTable item in querySegment.Result) {
                    maxTweetID = item.TweetID;
                }

            } catch (Exception ex) {
                
                log.Error($"Error: {ex.Message},{ex.StackTrace}");
                if (ex.InnerException != null)
                {
                    log.Error($"InnerException:  {ex.InnerException.Message}, {ex.InnerException.StackTrace}");
                }

            }
            log.Info($"MAX TWEET ID: {maxTweetID}");

            if (maxTweetID == -1) {
                // 動作指示がないため処理を行わずに抜ける。
                log.Info("end procedure.");
                return;
            }

            TableContinuationToken token = null;
            int allCount = 0;
            int allErrCount = 0;
            int maxInsertedTweetID = 0;
            do
            {
                int count = 0;
                int errCount = 0;
                var tableQuery = new TableQuery<TweetLocationTable>()
                    .Where(TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.GreaterThan, maxTweetID.ToString()));

                var querySegment = inputTable.ExecuteQuerySegmentedAsync(tableQuery, token);
                foreach (TweetLocationTable item in querySegment.Result)
                {
                    count++;
                    try
                    {
                        log.Info($"Data loaded: '{item.RowKey}' | '{item.ScreenName}' | '{item.Url}'");
                        string realUrl = "";
                        if (!string.IsNullOrEmpty(item.Url))
                        {

                            try
                            {
                                if (urlCache.ContainsKey(item.Url))
                                {
                                    // log.Info("get url from cache.");
                                    realUrl = urlCache[item.Url];
                                }
                                else
                                {
                                    WebRequest req = WebRequest.Create(item.Url);

                                    var res = req.GetResponse();
                                    realUrl = res.ResponseUri.AbsoluteUri;

                                    urlCache[item.Url] = realUrl;
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

                                maxTweetID = Math.Max(maxTweetID, item.TweetID);

                            }
                            catch (Exception ex)
                            {
                                errCount++;
                                log.Error($"Exception: {ex.Message},{ex.StackTrace}");

                                if (ex.InnerException != null)
                                {
                                    log.Error($"InnerException:  {ex.InnerException.Message}, {ex.InnerException.StackTrace}");
                                }
                            }
                        }


                    }
                    catch (Exception ex)
                    {
                        log.Error($"Insert Error: {ex.Message},{ex.StackTrace}");
                        if (ex.InnerException != null)
                        {
                            log.Error($"InnerException:  {ex.InnerException.Message}, {ex.InnerException.StackTrace}");
                        }
                    }
                }

                log.Info($"Fetch: {count},{errCount}");

                allCount += count;
                allErrCount += errCount;

                token = querySegment.Result.ContinuationToken;

            } while (token != null);

            log.Info($"Insert Done MaxTweetID: {maxInsertedTweetID} AllCount: {allCount} AllErrorCount: {allErrCount}");

            try
            {
                var task = maxIDTable.ExecuteAsync(TableOperation.Replace(new TweetMaxIDTable()
                {
                    PartitionKey = "k1",
                    RowKey = "max",
                    ETag = "*",
                    TweetID = maxInsertedTweetID
                }));
            }
            catch (Exception ex)
            {
                log.Error($"Error: {ex.Message},{ex.StackTrace}");
                if (ex.InnerException != null)
                {
                    log.Error($"InnerException:  {ex.InnerException.Message}, {ex.InnerException.StackTrace}");
                }
            }

            log.Info($"MaxTweetID Update Done. {maxInsertedTweetID}");

        }
    }
}
