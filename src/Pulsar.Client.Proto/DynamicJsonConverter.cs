using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Serialization;

// based on https://github.com/dotnet/runtime/issues/29690#issuecomment-571969037
namespace Pulsar.Client.Common
{
    public class DynamicJsonConverter : JsonConverter<IDictionary<string, object>>
    {
        public override IDictionary<string, object> Read(ref Utf8JsonReader reader,
            Type typeToConvert,
            JsonSerializerOptions options)
        {
            if (reader.TokenType == JsonTokenType.StartObject)
            {
                using JsonDocument documentV = JsonDocument.ParseValue(ref reader);
                return ReadObject(documentV.RootElement);
            }

            throw new Exception("Unsupported schema type");
        }

        private IDictionary<string, object> ReadObject(JsonElement jsonElement)
        {
            IDictionary<string, object> expandoObject = new Dictionary<string, object>();
            foreach (var obj in jsonElement.EnumerateObject())
            {
                var k = obj.Name;
                var value = ReadValue(obj.Value);
                expandoObject[k] = value;
            }
            return expandoObject;
        }
        private object ReadValue(JsonElement jsonElement)
        {
            object result = null;
            switch (jsonElement.ValueKind)
            {
                case JsonValueKind.Object:
                    result = ReadObject(jsonElement);
                    break;
                case JsonValueKind.Array:
                    result = ReadList(jsonElement);
                    break;
                case JsonValueKind.String:
                    //TODO: Missing Datetime&Bytes Convert
                    result = jsonElement.GetString();
                    break;
                case JsonValueKind.Number:
                    //TODO: more num type
                    if (jsonElement.TryGetDouble(out double d))
                    {
                        result = d;
                    }
                    else
                    {
                        result = 0.0;
                    }
                    break;
                case JsonValueKind.True:
                    result = true;
                    break;
                case JsonValueKind.False:
                    result = false;
                    break;
                case JsonValueKind.Undefined:
                case JsonValueKind.Null:
                    result = null;
                    break;
                default:
                    throw new ArgumentOutOfRangeException("ValueKind", "Unknown JsonValueKind: " + jsonElement.ValueKind);
            }
            return result;
        }

        private object ReadList(JsonElement jsonElement)
        {
            IList<object> list = new List<object>();
            foreach (var item in jsonElement.EnumerateArray())
            {
                list.Add(ReadValue(item));
            }
            return list.Count == 0 ? null : list;
        }
        public override void Write(Utf8JsonWriter writer,
            IDictionary<string, object> value,
            JsonSerializerOptions options)
        {
           // writer.WriteStringValue(value.ToString());
        }
    }
}