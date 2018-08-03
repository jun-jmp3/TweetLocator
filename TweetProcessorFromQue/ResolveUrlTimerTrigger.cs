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
using System.Linq;

namespace TweetProcessorFromQue
{
    public static class ResolveUrlTimerTrigger
    {

        private static Dictionary<string, string> urlCache = new Dictionary<string, string>();
        private static Dictionary<string, string> charaNameTable = null;

        [FunctionName("ResolveUrlTimerTrigger")]
        public static void Run(
            [TimerTrigger("0 1 * * * *")]TimerInfo myTimer, 
            [Table("TweetLocation", Connection = "AzureWebJobsStorage")] CloudTable inputTable,
            [Table("TweetMaxID", Connection = "AzureWebJobsStorage")] CloudTable maxIDTable,
            [Table("OnmusuCharactorName", Connection = "AzureWebJobsStorage")] CloudTable onmusuTable,
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

            if (maxTweetID == -1)
            {
                // 動作指示がないため処理を行わずに抜ける。
                log.Info("end procedure.");
                return;
            }

            // charatableを取得
            if (charaNameTable == null)
            {
                log.Info($"Creating charactor table.");

                try
                {
                    charaNameTable = new Dictionary<string, string>();
                    var tableQuery = new TableQuery<CharactorTable>();

                    var querySegment = onmusuTable.ExecuteQuerySegmentedAsync(tableQuery, null);
                    foreach (CharactorTable item in querySegment.Result)
                    {
                        charaNameTable[item.SearchName] = item.FullName;
                    }

                }
                catch (Exception ex)
                {
                    log.Error($"Error: {ex.Message},{ex.StackTrace}");
                    if (ex.InnerException != null)
                    {
                        log.Error($"InnerException:  {ex.InnerException.Message}, {ex.InnerException.StackTrace}");
                    }

                }

                log.Info($"Creating charactor table done. [{charaNameTable.Count}]");
            }

            TableContinuationToken token = null;
            int allCount = 0;
            int allErrCount = 0;
            long maxInsertedTweetID = 0;
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
                        log.Info($"Data loaded: '{item.RowKey}' | '{item.TweetID}' | '{item.ScreenName}' | '{item.Url}'");
                        maxInsertedTweetID = Math.Max(maxInsertedTweetID, item.TweetID);

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

                                var charaName = GetCharactorName(item.Text, log);

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
                                    Charactor = charaName,
                                    Location = item.Location,
                                    PlaceID = item.PlaceID,
                                    Latitude = item.Latitude,
                                    Longitude = item.Longitude
                                });


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
                if (maxInsertedTweetID != 0)
                {
                    var task = maxIDTable.ExecuteAsync(TableOperation.Replace(new TweetMaxIDTable()
                    {
                        PartitionKey = "k1",
                        RowKey = "max",
                        ETag = "*",
                        TweetID = maxInsertedTweetID
                    }));
                }
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


        private static string GetCharactorName(string text, TraceWriter log)
        {
            string result = "";
            try
            {

                foreach(string searchName in charaNameTable.Keys)
                {
                    if (text.Contains(searchName))
                    {
                        result = charaNameTable[searchName];
                        break;
                    }
                }

            }catch(Exception ex)
            {
                log.Error($"Error: {ex.Message},{ex.StackTrace}");
                if (ex.InnerException != null)
                {
                    log.Error($"InnerException:  {ex.InnerException.Message}, {ex.InnerException.StackTrace}");
                }

            }

            return result;
        }


    }
}
