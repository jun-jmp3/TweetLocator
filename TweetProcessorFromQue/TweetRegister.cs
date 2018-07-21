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
    public class TweetRegister
    {
        public TweetRegister()
        {
        }


        /// <summary>
        /// Inserts the record.
        /// </summary>
        public void InsertRecord(ICollector<TweetLocationTable> locationTable,
                                         Status status,
                                         PlaceResponse placeResponse,
                                         TraceWriter log)
        {
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
//                RowKey = randomStr,
                RowKey = status.Id.ToString(),
                TweetID = status.Id,
                TweetTime = status.CreatedAt.UtcDateTime,
                UserID = status.User.Id,
                ScreenName = status.User.ScreenName,
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
