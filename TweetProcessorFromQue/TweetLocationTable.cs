using System;
namespace TweetProcessorFromQue
{
    public class TweetLocationTable
    {
        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public long TweetID { get; set; }
        public long? UserID { get; set; }
        public string ScreenName { get; set; }
        public DateTime TweetTime { get; set; }
        public string Text { get; set; }
        public string Location { get; set; }
        public string PlaceID { get; set; }
        public double Latitude { get; set; }
        public double Longitude { get; set; }
    }
}
