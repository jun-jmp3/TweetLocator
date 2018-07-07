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

    public class TweetLocationTable {
        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public long TweetID { get; set; }
        public DateTime TweetTime { get; set; }
        public string Text { get; set; }
        public double Latitude { get; set; }
        public double Longitude { get; set; }
    }

    public static class QueueTrigger
    {

        [FunctionName("QueueTrigger")]
        public static void Run(
            [QueueTrigger("wug-tweets-que", Connection = "AzureWebJobsStorage")] string myQueueItem, 
            [Table("TweetLocation", Connection = "AzureWebJobsStorage")] ICollector<TweetLocationTable> locationTable,
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

                // ツイートを記録する
                InsertRecord(locationTable, status, log);

            }
            catch (Exception ex)
            {
                log.Error($"{ex.Message}");
                log.Error($"{ex.StackTrace}");
            }
        }

        /// <summary>
        /// Inserts the record.
        /// </summary>
        private static void InsertRecord(ICollector<TweetLocationTable> locationTable,
                                         StatusResponse status,
                                         TraceWriter log) {
            
            double latitude = 0.0;
            double longitude = 0.0;
            var place = status.Place;
            if (status.Coordinates != null || place != null)
            {
                log.Info($"{status.User.ScreenName} {status.Text}");
            }



            if (status.Coordinates != null)
            {
                log.Info($"Status Coordinates: {status.Coordinates.Longitude},{status.Coordinates.Latitude}");
            }

            if (place != null)
            {
                log.Info($"Place: {place.FullName},{place.Id}");

                if (place.Centroid != null)
                {
                    foreach (var oid in place.Centroid)
                        log.Info($"Centoroid {oid}");
                }

                if (place.Geometry != null)
                {
                    log.Info($"Long: {place.Geometry.Longitude}, Lat:{place.Geometry.Latitude}");
                }
            }

            // RowKeyの重複回避のためランダムな文字列を生成する
            Random random = new Random();
            string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
            string randomStr = new string(Enumerable.Repeat(chars, 32)
                .Select(s => s[random.Next(s.Length)]).ToArray());

            // SampleTableへEntity(レコード)登録
            locationTable.Add(new TweetLocationTable()
            {
                PartitionKey = "k1",
                RowKey = randomStr,
                TweetID = status.Id,
                TweetTime = status.CreatedAt.UtcDateTime,
                Text = status.Text,
                Latitude = latitude,
                Longitude = longitude
            }
                             );


        }

    }
}
