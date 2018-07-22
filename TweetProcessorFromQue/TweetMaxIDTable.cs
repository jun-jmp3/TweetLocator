using System;
using Microsoft.WindowsAzure.Storage.Table;

namespace TweetProcessorFromQue
{
    public class TweetMaxIDTable : TableEntity
    {
        public long TweetID { get; set; }

    }
}
