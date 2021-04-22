namespace Pulsar.Client.Schema


open System
open System.IO
open System.Text
open System.Text.Json
open Google.Protobuf.Reflection
open ProtoBuf
open Pulsar.Client.Api
open Pulsar.Client.Common

[<ProtoContract>]
type ExtensibleClass()=
    inherit Extensible()
type   ProtobufNativeSchemaData(fileDescriptorSet:byte[],
                                       rootMessageTypeName: string,
                                       rootFileDescriptorName:string) =      
    member this.fileDescriptorSet = fileDescriptorSet       
    member  this.rootMessageTypeName =rootMessageTypeName     
    member  this.rootFileDescriptorName =rootFileDescriptorName
type VirtualFile(content:string)=
    member private this.Content = content
    interface IFileSystem with
        member this.Exists _ = true
        member this.OpenText _ =
            new StringReader(this.Content) :> TextReader
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
               
                let protoForType = Serializer.GetProto<'T> () 
                let set = FileDescriptorSet( FileSystem = VirtualFile(protoForType))
                let protoFileName = userClassName + ".proto"
                let baseUri =  Uri("file://" + protoFileName, UriKind.Absolute)
                set.AddImportPath(baseUri.AbsolutePath)
                set.Add protoFileName |> ignore
                set.Process()
                
                use stream = MemoryStreamManager.GetStream()
                Serializer.Serialize(stream, set)                  
            
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
        
        
type internal GenericProtobufNativeSchema(topicSchema: TopicSchema) =
    inherit ISchema<GenericRecord>()
    
    override this.Encode _ = raise <| SchemaSerializationException "GenericProtobufNativeSchema is for consuming only!"   
    override this.SchemaInfo = {
        Name = ""
        Type = SchemaType.PROTOBUF_NATIVE
        Schema = topicSchema.SchemaInfo.Schema
        Properties = Map.empty
    }
    
    override this.Decode bytes =
        let schemaVersionBytes =
            topicSchema.SchemaVersion
            |> Option.map (fun sv -> sv.Bytes)
            |> Option.toObj
            
        let schemaSpan = ReadOnlySpan(topicSchema.SchemaInfo.Schema)         
        let data = JsonSerializer.Deserialize schemaSpan :> ProtobufNativeSchemaData        
        let descriptorSpan = ReadOnlySpan(data.fileDescriptorSet)
        let desc = Serializer.Deserialize<FileDescriptorSet>  descriptorSpan
        let extensibleSpan = ReadOnlySpan(bytes)
        let ext = Serializer.Deserialize<ExtensibleClass>  extensibleSpan
        
        let file = desc.Files.[0] //we don't allow multi-file for now anyway
        let messageFields = file.MessageTypes.[0].Fields               
       
        let getFieldsFromMessage (field:FieldDescriptorProto) : Field =
            let fieldCreator value =
                { Name = field.Name; Value = value; Index = field.Number }            
            match field.``type`` with
              
               | FieldDescriptorProto.Type.TypeDouble -> 
                   Extensible.GetValue<double>(ext, field.Number) |> fieldCreator
               | FieldDescriptorProto.Type.TypeFloat -> 
                   Extensible.GetValue<float>(ext, field.Number) |> fieldCreator
               | FieldDescriptorProto.Type.TypeInt64
               | FieldDescriptorProto.Type.TypeFixed64  //https://stackoverflow.com/questions/837537/protocol-buffers-should-i-use-int64-or-fixed64-to-represent-a-net-datetime-val
               | FieldDescriptorProto.Type.TypeSfixed64
               | FieldDescriptorProto.Type.TypeSint64 -> 
                   Extensible.GetValue<int64>(ext, field.Number) |> fieldCreator                   
               | FieldDescriptorProto.Type.TypeUint64 ->
                   Extensible.GetValue<uint64>(ext, field.Number)  |> fieldCreator
               | FieldDescriptorProto.Type.TypeInt32
               | FieldDescriptorProto.Type.TypeFixed32
               | FieldDescriptorProto.Type.TypeSint32
               | FieldDescriptorProto.Type.TypeSfixed32  -> 
                   Extensible.GetValue<int32>(ext, field.Number) |> fieldCreator     
               | FieldDescriptorProto.Type.TypeBool -> 
                   Extensible.GetValue<bool >(ext, field.Number)  |> fieldCreator
               | FieldDescriptorProto.Type.TypeString ->                
                   Extensible.GetValue<string >(ext, field.Number)  |> fieldCreator
               | FieldDescriptorProto.Type.TypeBytes -> 
                   Extensible.GetValue<byte[] >(ext, field.Number)  |> fieldCreator
               | FieldDescriptorProto.Type.TypeUint32 -> 
                   Extensible.GetValue<uint32 >(ext, field.Number)  |> fieldCreator
               | FieldDescriptorProto.Type.TypeEnum -> 
                   Extensible.GetValue<Enum>(ext, field.Number)  |> fieldCreator                       
               | _ -> failwith "not supported yet" //TYPE_MESSAGE
               
        let fields =
             messageFields
            |> Seq.map getFieldsFromMessage
            |> Seq.toArray   

        GenericRecord(schemaVersionBytes, fields)