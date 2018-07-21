using System;
using Microsoft.WindowsAzure.Storage.Table;

namespace TweetProcessorFromQue
{
    public class TweetLocationTable : TableEntity
    {
        public long TweetID { get; set; }
        public long? UserID { get; set; }
        public string ScreenName { get; set; }
        public DateTime TweetTime { get; set; }
        public string Text { get; set; }
        public string Url { get; set; }
        public string Location { get; set; }
        public string PlaceID { get; set; }
        public double Latitude { get; set; }
        public double Longitude { get; set; }
    }
}
