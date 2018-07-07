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
        public string Location { get; set; }
        public string PlaceID { get; set; }
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

                PlaceResponse place = null;
                if (status.Place != null) {

                    var task2 = tokens.Geo.IdAsync(status.Place.Id);
                    place = task2.Result;
                    
                }
                // ツイートを記録する
                InsertRecord(locationTable, status, place, log);

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
                                         PlaceResponse placeResponse,
                                         TraceWriter log) {
            string location = "";
            double latitude = 0.0;
            double longitude = 0.0;
            string placeID = "";

            if (status.Coordinates != null)
            {
                latitude = status.Coordinates.Latitude;
                longitude = status.Coordinates.Longitude;
            }

            if (placeResponse != null)
            {
                location = placeResponse.FullName;
                placeID = placeResponse.Id;
                if (placeResponse.Centroid != null && placeResponse.Centroid.Length >= 2)
                {
                    latitude = placeResponse.Centroid[1];
                    longitude = placeResponse.Centroid[0];
                }

                if (placeResponse.Geometry != null)
                {
                    latitude = placeResponse.Geometry.Latitude;
                    longitude = placeResponse.Geometry.Longitude;
                }
            }

            var place = status.Place;
            if (place != null)
            {
                location = place.FullName;
                placeID = place.Id;

                if (place.Centroid != null && place.Centroid.Length >= 2)
                {
                    latitude = place.Centroid[1];
                    longitude = place.Centroid[0];
                }
                if (place.Geometry != null)
                {
                    latitude = place.Geometry.Latitude;
                    longitude = place.Geometry.Longitude;
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
                Location = location,
                PlaceID = placeID,
                Latitude = latitude,
                Longitude = longitude
            }
                             );


        }

    }
}
