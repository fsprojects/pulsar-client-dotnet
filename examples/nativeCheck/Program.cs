using System;
using System.IO;
using System.Runtime.Serialization;
using Google.Protobuf.Reflection;
using ProtoBuf;


namespace nativeCheck
{
    [DataContract]
    public class XXXMessage
    {
        [DataMember(Order = 1)]
        public string foo { get; set; }
        [DataMember(Order = 2)]
        public double bar { get; set; }
        
    }
    class Program
    {
        static string MakeRelativePath(string fromPath, string toPath)
        {
            return Path.GetRelativePath(fromPath, toPath);
        }
        static void Main(string[] args)
        {   
            var set = new FileDescriptorSet();
            var newfile = Path.GetTempFileName();
            var s = Serializer.GetProto<XXXMessage>();
            File.WriteAllText(newfile,s);
            
            var currentPath = Path.GetDirectoryName(typeof(Program).Assembly.Location);
            var newPath = Path.Combine(currentPath, Path.GetFileName(newfile));
            File.Copy(newfile,newPath);
            if (File.Exists(Path.Combine(currentPath, "Test.proto")))
                File.Delete(Path.Combine(currentPath, "Test.proto"));
            File.Move(newPath, Path.Combine(currentPath,"Test.proto"));
            newPath = Path.Combine(currentPath, "Test.proto");
            
            var newfile2 = Path.GetTempFileName();
            var newPath2 = Path.Combine(currentPath, Path.GetFileName(newfile2));
            File.Copy(newfile2,newPath2);
            
            //magic starts here
            set.AddImportPath(currentPath);
            var relativePath = MakeRelativePath(currentPath, newPath);
            set.Add(relativePath);
            set.Process();
          
            using (var fds = File.Open(newPath2,FileMode.Open))
            {
                Serializer.Serialize(fds, set);
            }
            //ends here
            
            File.Delete(newfile);
            File.Delete(newPath);
          
            var bytes = File.ReadAllBytes(newPath2);
            var q =Convert.ToBase64String(bytes);
            Console.WriteLine(q);
           
            Console.ReadKey();
        }
    }
}