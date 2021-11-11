using System;
using System.Collections.Generic;
using System.IO;
using Google.Protobuf.Reflection;
using ProtoBuf.Reflection;

namespace ProtobufNativeGenerated
{
    /// <summary>
    /// It's a mess to provide relative path for codegen; I decided to skip copying input file next to executable
    /// and deleting it afterwards with virtual file approach  
    /// https://github.com/protobuf-net/protobuf-net/issues/508
    /// </summary>
    class VirtualFile : IFileSystem
    {
        private readonly string _content;
        public VirtualFile(string path)
        {
            var input = Path.GetFullPath(path);
            _content = File.ReadAllText(input);
        }
        public bool Exists(string path)
        {
            return true;
        }

        public TextReader OpenText(string path)
        {
            return new StringReader(_content);
        }
    }
    class Program
    {
        static void Main(string[] args)
        {
            CodeGenerator codegen = CSharpCodeGenerator.Default;
            var set = new FileDescriptorSet{FileSystem = new VirtualFile(@"../../../sample.proto")};
            var protoFileName = "sample.proto"; //https://developers.google.com/protocol-buffers/docs/proto3
            var baseUri = new Uri("file://" + protoFileName, UriKind.Absolute);
            set.AddImportPath(baseUri.AbsolutePath);
            set.Add(protoFileName);
            set.Process();
            var files = codegen.Generate(set);
            WriteFiles(files, @"../../../");
          
        }
        static void WriteFiles(IEnumerable<CodeFile> files, string outPath)
        {
            foreach (var file in files)
            {
                var path = Path.Combine(outPath, file.Name);

                var dir = Path.GetDirectoryName(path);
                if (!Directory.Exists(dir))
                {
                    Console.Error.WriteLine($"Output directory does not exist, creating... {dir}");
                    Directory.CreateDirectory(dir);
                }

                File.WriteAllText(path, file.Text);
                Console.WriteLine($"generated: {Path.GetFullPath(path)}");
            }
        }
    }
}