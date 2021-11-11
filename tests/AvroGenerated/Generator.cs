using System.IO;
using System.Text;
using Avro;

namespace AvroGenerated
{
    public class Generator
    {
        private static void Main(string[] args)
        {
            string[] allLines = File.ReadAllLines(@"../../../sample.avsc", Encoding.UTF8);

            var schemaJson = string.Join("", allLines);

            var avroSchema = Schema.Parse(schemaJson);
            var codeGen = new CodeGen();
            codeGen.AddSchema(avroSchema);
            codeGen.GenerateCode();
            codeGen.WriteTypes(@"../../../../");    
        }
        
    }
}