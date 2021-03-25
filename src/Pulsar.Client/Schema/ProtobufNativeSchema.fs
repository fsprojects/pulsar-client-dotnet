namespace Pulsar.Client.Schema


open System
open System.Collections.Generic
open System.IO
open System.Text
open System.Text.Json
open Google.Protobuf.Reflection
open ProtoBuf
open Pulsar.Client.Api
open Pulsar.Client.Common

 type   ProtobufNativeSchemaData(fileDescriptorSet:byte[],
                                       rootMessageTypeName: string,
                                       rootFileDescriptorName:string) =
    
    //protobuf v3 FileDescriptorSet bytes.   
    member this.fileDescriptorSet = fileDescriptorSet
    
    //protobuf v3 rootMessageTypeName   
    member  this.rootMessageTypeName =rootMessageTypeName
  
    //protobuf v3 rootFileDescriptorName
    member  this.rootFileDescriptorName =rootFileDescriptorName

type internal ProtoBufNativeSchema<'T > () =
    inherit ISchema<'T>()    
  
    let getDescriptor( )=       
       
        let gotClassAttribute = Attribute.GetCustomAttributes(typeof<'T>) |>
                                Array.tryFind
                                    (fun x -> x :? System.Runtime.Serialization.DataContractAttribute)
        match gotClassAttribute with
            Some _ ->
                let userClassNamespace = typeof<'T>.Namespace
                let userClassName = typeof<'T>.Name
                
                let set = FileDescriptorSet()
                let protoForType = Serializer.GetProto<'T> () //save it to .proto file nearby
                let currentPath = Path.GetDirectoryName(typeof<'T>.Assembly.Location)       
                let protoFileName = userClassName + ".proto"
                let protoFilePath = Path.Combine (currentPath , protoFileName)
                File.WriteAllText ( protoFilePath , protoForType)
                
                set.AddImportPath(currentPath)
                set.Add protoFileName |> ignore
                set.Process()
                
                use stream = MemoryStreamManager.GetStream()
                Serializer.Serialize(stream, set)                    
            
                File.Delete protoFilePath
                
               
                ProtobufNativeSchemaData (stream.ToArray (), userClassNamespace + "." + userClassName, protoFileName)
               //nativeCheck2.XXXMessage XXXMessage.proto
            | _ -> raise (Exception("Please decorate message class and it's members with protobuf attributes"))


    let stringSchema () =
        let q = getDescriptor () |> JsonSerializer.Serialize |> Encoding.UTF8.GetBytes  
        File.WriteAllBytes ("D:\\codedFromFsharp",  q)
        q
    let parameterIsClass =  typeof<'T>.IsClass    
    override this.Decode bytes =
        use stream = new MemoryStream(bytes)   
        Serializer.Deserialize(stream)
        
    override this.Encode value =            
        if parameterIsClass && (isNull <| box value) then
            raise <| SchemaSerializationException "Need Non-Null content value"
        use stream = MemoryStreamManager.GetStream()      
        Serializer.Serialize(stream, value)       
        stream.ToArray()  

    override this.SchemaInfo =
        {
        Name = ""
        Type = SchemaType.PROTOBUF_NATIVE
        Schema = stringSchema() 
        Properties = Map.empty
    }
        