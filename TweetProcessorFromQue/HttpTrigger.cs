
using System.IO;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Azure.WebJobs.Host;
using Newtonsoft.Json;

using Microsoft.Extensions.Configuration;

using CoreTweet;

namespace TweetProcessorFromQue
{
    public static class HttpTrigger
    {
        [FunctionName("HttpTrigger")]
        public static IActionResult Run([HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)]HttpRequest req,
                                        TraceWriter log,
                                        ExecutionContext context)
        {
            log.Info("C# HTTP trigger function processed a request.");

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

            string keyword = req.Query["KeyWord"];

            /*
            string name = req.Query["name"];

            string requestBody = new StreamReader(req.Body).ReadToEnd();
            dynamic data = JsonConvert.DeserializeObject(requestBody);
            name = name ?? data?.name;

            return name != null
                ? (ActionResult)new OkObjectResult($"Hello, {name}")
                : new BadRequestObjectResult("Please pass a name on the query string or in the request body");
                */

            return keyword != null
                ? (ActionResult)new OkObjectResult($"Hello, {keyword}")
                    : new BadRequestObjectResult("Please pass a keyword on the query string or in the request body");

        }
    }
}
