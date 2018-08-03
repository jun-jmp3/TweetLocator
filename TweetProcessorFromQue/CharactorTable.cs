using System;
using Microsoft.WindowsAzure.Storage.Table;

namespace TweetProcessorFromQue
{
    class CharactorTable: TableEntity
    {
        public string FullName { get; set; }
        public string SearchName { get; set; }


    }
}
