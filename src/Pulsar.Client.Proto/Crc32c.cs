using System.IO;
using System.Numerics;
using Microsoft.IO;


namespace Pulsar.Client.Common
{
    internal static class CRC32C
    {
        internal static uint GetForRMS(RecyclableMemoryStream stream, int size)
        {
            var crc = ~0U; //0xFFFFFFFF
            var memorySequence = stream.GetReadOnlySequence().Slice(stream.Position);
            foreach (var memory in memorySequence)
            {
                var span = memory.Span;
                var currentBlockLength = span.Length;
                var i = 0;
                while (size-- > 0 && i < currentBlockLength)
                {
                    crc = BitOperations.Crc32C(crc, span[i]);
                    i++;
                }
            }
            return crc ^ ~0U; //0xFFFFFFFF
        }

        internal static uint GetForMS(MemoryStream stream, int size)
        {
            var crc = ~0U; //0xFFFFFFFF
            var buf = stream.GetBuffer();
            var offset = stream.Position;
            for (var i = 0; i < size; i++)
                crc = BitOperations.Crc32C(crc, buf[offset+i]);

            return crc ^ ~0U; //0xFFFFFFFF
        }
    }
}

