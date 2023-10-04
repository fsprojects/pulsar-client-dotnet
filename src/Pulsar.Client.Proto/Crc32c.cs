using System;
using System.Buffers;
using System.IO;
using System.Numerics;
using Microsoft.IO;


namespace Pulsar.Client.Common
{
    internal static class CRC32C
    {
        internal static uint GetForROS(ReadOnlySequence<byte> stream)
        {
            var crc = ~0U; //0xFFFFFFFF
            foreach (var memory in stream)
            {
                var span = memory.Span;
                CrcAlgorithm(span, ref crc);
            }
            return crc ^ ~0U; //0xFFFFFFFF
        }

        private static void CrcAlgorithm(ReadOnlySpan<byte> span, ref uint crc)
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
            while (i < currentBlockLength)
            {
                crc = BitOperations.Crc32C(crc, span[i]);
                i++;
            }
        }
    }
}

