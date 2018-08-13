using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Azure.WebJobs.Host;
using Newtonsoft.Json;

using System.Collections.Generic;
using Microsoft.Extensions.Configuration;

using CoreTweet;
using CoreTweet.Core;
using CoreTweet.Rest;

namespace TweetProcessorFromQue
{
    public static class HttpTrigger
    {
        [FunctionName("HttpTrigger")]
        public static IActionResult Run([HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)]HttpRequest req,
                                        [Table("TweetLocation", Connection = "AzureWebJobsStorage")] ICollector<TweetLocationTable> locationTable,
                                        TraceWriter log,
                                        ExecutionContext context)
        {
            log.Info("C# HTTP trigger function processed a request.");
            int allCount = 0;

            try
            {
                // 環境変数を読み込む
                var config = new ConfigurationBuilder()
                    .SetBasePath(context.FunctionAppDirectory)
                    .AddJsonFile("local.settings.json", optional: true, reloadOnChange: true)
                    .AddEnvironmentVariables()
                    .Build();

                // 環境変数からキーを取得する
                string consumerKey = config["ConsumerKey"];
                string consumerSecret = config["ConsumerSecret"];
                string accessToken = config["AccessToken"];
                string accessTokenSecret = config["AccessTokenSecret"];

                if (string.IsNullOrEmpty(consumerKey) || string.IsNullOrEmpty(consumerSecret) || string.IsNullOrEmpty(accessToken) || string.IsNullOrEmpty(accessTokenSecret))
                {
                    log.Error("can't read configuration about token.");
                    return new BadRequestObjectResult("can't read configuration about token.");
                }

                string query = req.Query["query"];
                if (string.IsNullOrEmpty(query))
                {
                    log.Error("can't read configuration about token.");
                    return new BadRequestObjectResult("Please pass a keyword on the query string or in the request body");
                }

                string countParam = req.Query["count"];
                int count = 100;
                if (!string.IsNullOrEmpty(countParam))
                {
                    count = Convert.ToInt32(countParam);

                }

                string since = req.Query["since"];
                string until = req.Query["until"];
                long since_id = 0;
                if (!string.IsNullOrEmpty(req.Query["since_id"])) {
                    since_id = Convert.ToInt64(req.Query["since_id"]);
                }
                long max_id = Int64.MaxValue;
                if (!string.IsNullOrEmpty(req.Query["max_id"]))
                {
                    max_id = Convert.ToInt64(req.Query["max_id"]);
                }

                /*
                string name = req.Query["name"];

                string requestBody = new StreamReader(req.Body).ReadToEnd();
                dynamic data = JsonConvert.DeserializeObject(requestBody);
                name = name ?? data?.name;

                return name != null
                    ? (ActionResult)new OkObjectResult($"Hello, {name}")
                    : new BadRequestObjectResult("Please pass a name on the query string or in the request body");
                    */

                /*
                                 var task = tokens.Statuses.ShowAsync(id => myQueueItem);
                                var status = task.Result;
                */

                int availableThread, availableCompletionPortThread;
                long minID = long.MaxValue;
                while (true)
                {
                    log.Info($"{query}, {count}, {since}, {until}, {since_id}, {max_id - 1}");
                    // log.Info($"{query}, {count}, {since_id}, {max_id - 1}");
                    // Search Tweets
                    // アクセストークン
                    var tokens = Tokens.Create(consumerKey, consumerSecret, accessToken, accessTokenSecret);
                    Search search = tokens.Search;
                    Dictionary<string, object> metaDict = new Dictionary<string, object>()
                    {
                        {"q", query},
                        {"count", count},
                        {"exclude_replies", true},
                        {"since_id", since_id},
                        {"max_id", max_id - 1 }
                    };

                    if (!string.IsNullOrEmpty(since))
                    {
                        metaDict.Add("since", since);
                    }

                    if (!string.IsNullOrEmpty(until))
                    {
                        metaDict.Add("until", until);
                    }

                    var task = search.TweetsAsync(metaDict);

                    SearchResult searchResult = task.Result;
                    TweetRegister register = new TweetRegister();
                    foreach (Status tweet in searchResult)
                    {
                        System.Threading.ThreadPool.GetAvailableThreads(out availableThread, out availableCompletionPortThread);
                        log.Info($"Process tweet id: {tweet.Id} availableThread: {availableThread}");

                        PlaceResponse place = null;
                        if (tweet.Place != null)
                        {

                            var task2 = tokens.Geo.IdAsync(tweet.Place.Id);
                            place = task2.Result;

                        }

                        // ツイートを記録する
                        var task3 = Task.Run(() =>
                        {
                            register.InsertRecord(locationTable, tweet, place, log);
                        });

                        minID = System.Math.Min(minID, tweet.Id);
                    }

                    allCount += searchResult.Count;

                    // 取得最大値と取得できたカウントが一致した場合、繰り返す
                    if (searchResult.Count == System.Convert.ToInt32(count))
                    {
                        max_id = minID;
                    }
                    else
                    {
                        break;
                    }

                }
            }catch (Exception ex) {
                log.Error($"Exception: {ex.Message}, {ex.StackTrace}");

                if (ex.InnerException != null) {
                    log.Error($"InnerException:  {ex.InnerException.Message}, {ex.InnerException.StackTrace}");
                }

            }

            return (ActionResult)new OkObjectResult($"{DateTime.Now.ToString()}, {allCount}");

        }
    }
}
