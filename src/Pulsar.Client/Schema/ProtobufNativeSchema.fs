namespace Pulsar.Client.Schema


open System
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
    member this.fileDescriptorSet = fileDescriptorSet       
    member  this.rootMessageTypeName =rootMessageTypeName     
    member  this.rootFileDescriptorName =rootFileDescriptorName

type internal ProtoBufNativeSchema<'T > () =
    inherit ISchema<'T>()    
  
    let getDescriptor( )=        
       
       
        let gotClassAttribute = Attribute.GetCustomAttributes(typeof<'T>) |>
                                Array.tryFind (fun x -> match x with
                                                        | :? System.Runtime.Serialization.DataContractAttribute -> true
                                                        | :? ProtoContractAttribute -> true
                                                        | _ -> false)
                                
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
            | _ -> raise (Exception("Please decorate message class and it's members with protobuf attributes"))


    let stringSchema () =
        getDescriptor () |> JsonSerializer.Serialize |> Encoding.UTF8.GetBytes  
       
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
        