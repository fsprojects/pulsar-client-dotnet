// taken from https://gist.github.com/automatonic/3725443

using System;
using System.IO;
using System.Text;

namespace Pulsar.Client.Common
{

    public static class MurmurHash3
    {
        //maximum allowed stackalloc size.
        const int MaximumStackAllocSize = 32 * 1024;

        public static int Hash(string value)
        {
            if (String.IsNullOrEmpty(value))
                return 0;
            
            int maxBytesCount = value.Length * sizeof(char);

            //fallback to default hash heap calculation when input string is too long.
            if (maxBytesCount > MaximumStackAllocSize)
                return HashWithAllocs(value);

            Span<byte> bytes = stackalloc byte[maxBytesCount];
            unsafe
            {
                fixed (char* inputChars = value)
                {
                    fixed (byte* fixedBytes = &bytes.GetPinnableReference())
                    {
                        int bytesCount = Encoding.UTF8.GetBytes(inputChars, value.Length, fixedBytes, bytes.Length);
                        return Hash(bytes, bytesCount, 0) & Int32.MaxValue;
                    }
                }
            }
        }
        public static int HashWithAllocs(string value)
        {
            if (String.IsNullOrEmpty(value))
                return 0;
            
            byte[] input = Encoding.UTF8.GetBytes(value);
            using (var stream = new MemoryStream(input))
                return Hash(stream, 0) & Int32.MaxValue;
        }

        //length is required as allocated buffer may be too big for actual values written to it.
        public static int Hash(Span<byte> bytes, int length, uint seed)
        {
            const uint c1 = 0xcc9e2d51;
            const uint c2 = 0x1b873593;

            uint h1 = seed;
            uint k1 = 0;
            uint parsedLength = 0;

            while (parsedLength < length)
            {
                Span<byte> chunk = bytes.Slice((int)parsedLength, Math.Min(length - (int)parsedLength,4));
                parsedLength += (uint)chunk.Length;

                switch (chunk.Length)
                {
                    case 4:
                        /* Get four bytes from the input into an uint */
                        k1 = (uint)
                           (chunk[0]
                          | chunk[1] << 8
                          | chunk[2] << 16
                          | chunk[3] << 24);

                        /* bitmagic hash */
                        k1 *= c1;
                        k1 = rotl32(k1, 15);
                        k1 *= c2;

                        h1 ^= k1;
                        h1 = rotl32(h1, 13);
                        h1 = h1 * 5 + 0xe6546b64;
                        break;
                    case 3:
                        k1 = (uint)
                           (chunk[0]
                          | chunk[1] << 8
                          | chunk[2] << 16);
                        k1 *= c1;
                        k1 = rotl32(k1, 15);
                        k1 *= c2;
                        h1 ^= k1;
                        break;
                    case 2:
                        k1 = (uint)
                           (chunk[0]
                          | chunk[1] << 8);
                        k1 *= c1;
                        k1 = rotl32(k1, 15);
                        k1 *= c2;
                        h1 ^= k1;
                        break;
                    case 1:
                        k1 = (uint)(chunk[0]);
                        k1 *= c1;
                        k1 = rotl32(k1, 15);
                        k1 *= c2;
                        h1 ^= k1;
                        break;

                }
            }

            // finalization, magic chants to wrap it all up
            h1 ^= parsedLength;
            h1 = fmix(h1);

            unchecked //ignore overflow
            {
                return (int)h1;
            }
        }

        public static int Hash(Stream stream, uint seed)
        {
            const uint c1 = 0xcc9e2d51;
            const uint c2 = 0x1b873593;

            uint h1 = seed;
            uint k1 = 0;
            uint streamLength = 0;

            using (BinaryReader reader = new BinaryReader(stream))
            {
                byte[] chunk = reader.ReadBytes(4);
                while (chunk.Length > 0)
                {
                    streamLength += (uint)chunk.Length;
                    switch (chunk.Length)
                    {
                        case 4:
                            /* Get four bytes from the input into an uint */
                            k1 = (uint)
                               (chunk[0]
                              | chunk[1] << 8
                              | chunk[2] << 16
                              | chunk[3] << 24);

                            /* bitmagic hash */
                            k1 *= c1;
                            k1 = rotl32(k1, 15);
                            k1 *= c2;

                            h1 ^= k1;
                            h1 = rotl32(h1, 13);
                            h1 = h1 * 5 + 0xe6546b64;
                            break;
                        case 3:
                            k1 = (uint)
                               (chunk[0]
                              | chunk[1] << 8
                              | chunk[2] << 16);
                            k1 *= c1;
                            k1 = rotl32(k1, 15);
                            k1 *= c2;
                            h1 ^= k1;
                            break;
                        case 2:
                            k1 = (uint)
                               (chunk[0]
                              | chunk[1] << 8);
                            k1 *= c1;
                            k1 = rotl32(k1, 15);
                            k1 *= c2;
                            h1 ^= k1;
                            break;
                        case 1:
                            k1 = (uint)(chunk[0]);
                            k1 *= c1;
                            k1 = rotl32(k1, 15);
                            k1 *= c2;
                            h1 ^= k1;
                            break;

                    }
                    chunk = reader.ReadBytes(4);
                }
            }

            // finalization, magic chants to wrap it all up
            h1 ^= streamLength;
            h1 = fmix(h1);

            unchecked //ignore overflow
            {
                return (int)h1;
            }
        }

        private static uint rotl32(uint x, byte r)
        {
            return (x << r) | (x >> (32 - r));
        }

        private static uint fmix(uint h)
        {
            h ^= h >> 16;
            h *= 0x85ebca6b;
            h ^= h >> 13;
            h *= 0xc2b2ae35;
            h ^= h >> 16;
            return h;
        }
    }
}
