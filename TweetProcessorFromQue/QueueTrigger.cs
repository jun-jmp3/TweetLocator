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
        public async static void Run([QueueTrigger("wug-tweets-que")]string myQueueItem, TraceWriter log, ExecutionContext context)
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

                string consumerKey = config["ConsumerKey"];
                string consumerSecret = config["ConsumerSecret"];
                string accessToken = config["AccessToken"];
                string accessTokenSecret = config["AccessTokenSecret"];

                var tokens = Tokens.Create(consumerKey, consumerSecret, accessToken, accessTokenSecret);
                // log.Info($"twitter token created: {tokens.ToString()}");

                var status = await tokens.Statuses.ShowAsync(id => myQueueItem);
                var place = status.Place;
                if (status.Coordinates != null || place != null)
                {
                    log.Info($"{status.User.ScreenName} {status.Text}");
                }
                if (status.Coordinates != null) {
                    log.Info($"Status Coordinates: {status.Coordinates.Longitude},{status.Coordinates.Latitude}");
                }

                if (place != null) {
                    log.Info($"Place: {place.FullName},{place.Id}");

                    if (place.Centroid != null) {                        
                        foreach (var oid in place.Centroid )
                            log.Info($"Centoroid {oid}");
                    }

                    if (place.Geometry != null)
                    {
                        log.Info($"Long: {place.Geometry.Longitude}, Lat:{place.Geometry.Latitude}");
                    }
                }
            }
            catch (Exception ex)
            {
                log.Error($"{ex.Message}");
                log.Error($"{ex.StackTrace}");
            }
        }
    }
}
