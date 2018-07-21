using System;
using System.Linq;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Configuration;

using CoreTweet;
using CoreTweet.Core;
using CoreTweet.Rest;

namespace TweetProcessorFromQue
{

    public static class QueueTrigger
    {

        [FunctionName("QueueTrigger")]
        public static void Run(
            [QueueTrigger("wug-tweets-que", Connection = "AzureWebJobsStorage")] string myQueueItem, 
            [Table("TweetLocation2", Connection = "AzureWebJobsStorage")] ICollector<TweetLocationTable> locationTable,
            TraceWriter log, 
            ExecutionContext context)
        {
            try
            {
                log.Info($"C# Queue trigger function processed: {myQueueItem}");

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

                if (string.IsNullOrEmpty(consumerKey) || string.IsNullOrEmpty(consumerSecret) || string.IsNullOrEmpty(accessToken) || string.IsNullOrEmpty(accessTokenSecret)) {
                    log.Error("can't read configuration about token.");
                    return;
                }

                // アクセストークン
                var tokens = Tokens.Create(consumerKey, consumerSecret, accessToken, accessTokenSecret);
                // log.Info($"twitter token created: {tokens.ToString()}");

                // ツイートを取得
                var task = tokens.Statuses.ShowAsync(id => myQueueItem);
                var status = task.Result;

                PlaceResponse place = null;
                if (status.Place != null) {

                    var task2 = tokens.Geo.IdAsync(status.Place.Id);
                    place = task2.Result;
                    
                }

                var register = new TweetRegister();
                // ツイートを記録する
                register.InsertRecord(locationTable, status, place, log);

            }
            catch (Exception ex)
            {
                log.Error($"{ex.Message}");
                log.Error($"{ex.StackTrace}");
            }
        }
    }
}
