using System;
using System.IO;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;

namespace TweetProcessorFromQue
{
    public class QueueTriggerFunction
    {
        public QueueTriggerFunction()
        {
        }

        [FunctionName("QueueTrigger")]
        public static void QueueTrigger(
            [QueueTrigger("wug-tweets-que")] string myQueueItem,
            TraceWriter log)
        {
            log.Info($"C# function processed: {myQueueItem}");
        }
    }
}
