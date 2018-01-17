using Microsoft.WindowsAzure.Storage.Table;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FunctionsLoadTesting
{
    public class Message : TableEntity
    {
        public string Text { get; set; }
    }
    public class Payload
    {
        public string Name { get; set; }
        public DateTime InsertedIntoQ1 { get; set; }
        public DateTime InsertedIntoQ2 { get; set; }
        public DateTime InsertedIntoQ3 { get; set; }
        public TimeSpan InsertIntervalAll { get; set; }
        public string Data { get; set; }
        public TimeSpan InsertInterval1 { get; set; }
        public TimeSpan InsertInterval2 { get; set; }

        public string ToText()
        {
            return JsonConvert.SerializeObject(this);
        }

        public static Payload FromText(string text)
        {
            return JsonConvert.DeserializeObject<Payload>(text);
        }
    }
}
