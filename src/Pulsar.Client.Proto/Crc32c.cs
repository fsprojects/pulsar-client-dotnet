using System;
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
                crc = CrcAlgorithm(size, span, crc);
            }
            return crc ^ ~0U; //0xFFFFFFFF
        }

        private static uint CrcAlgorithm(int size, ReadOnlySpan<byte> span, uint crc)
        {
            var currentBlockLength = span.Length;
            var i = 0;
            var bigStepsEnds = currentBlockLength - 8;
            while (i < bigStepsEnds)
            {
                var batch = BitConverter.ToUInt64(span.Slice(i, 8));
                crc = BitOperations.Crc32C(crc, batch);
                i+=8;
            }
            size -= i;
            while (size > 0 && i < currentBlockLength)
            {
                crc = BitOperations.Crc32C(crc, span[i]);
                size--;
                i++;
            }
            return crc;
        }

        internal static uint GetForMS(MemoryStream stream, int size)
        {
            var crc = ~0U; //0xFFFFFFFF
            var buf = stream.GetBuffer();
            var offset = (int) stream.Position;
            var span = buf.AsSpan(offset);
            crc = CrcAlgorithm(size, span, crc);
            return crc ^ ~0U; //0xFFFFFFFF
        }
    }
}

