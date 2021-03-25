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

type internal ProtoBufNativeSchema<'T > private (schema: string) =
    inherit ISchema<'T>()    
  
    let getDescriptor( )=       
       
        let gotClassAttribute = Attribute.GetCustomAttributes(typeof<'T>) |>
                                Array.tryFind
                                    (fun x -> x :? System.Runtime.Serialization.DataContractAttribute)
        match gotClassAttribute with
            Some _ ->
                let set = FileDescriptorSet()
                let protoForType = Serializer.GetProto<'T> () //save it to .proto file nearby
                let currentPath = Path.GetDirectoryName(typeof<'T>.Assembly.Location)       
                let protoFileName = Guid.NewGuid().ToString() + ".proto"
                let protoFilePath = Path.Combine (currentPath , protoFileName)
                File.WriteAllText ( protoFilePath , protoForType)
                
                set.AddImportPath(currentPath)
                set.Add protoFileName |> ignore
                set.Process()
                
                use stream = MemoryStreamManager.GetStream()
                Serializer.Serialize(stream, set)
                    
                let userClassNamespace = typeof<'T>.Namespace
                let userClassName = typeof<'T>.Name
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
    
    //если в сообщении какая-то другая схема указывается, то схема скачивается с сервера и по ней строится ISchema
    override this.GetSpecificSchema stringSchema =     
        ProtoBufNativeSchema<'T> (stringSchema) :> ISchema<'T>

    new()=
        ProtoBufNativeSchema<'T>(String.Empty)
    override this.SchemaInfo =
        {
        Name = ""
        Type = SchemaType.PROTOBUF_NATIVE
        Schema = stringSchema() 
        Properties = Map.empty
    }
        