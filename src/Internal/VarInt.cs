/*
VarintBitConverter:
https://github.com/topas/VarintBitConverter 
Copyright (c) 2011 Tomas Pastorek, Ixone.cz. All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions
are met:

 1. Redistributions of source code must retain the above copyright
    notice, this list of conditions and the following disclaimer.

 2. Redistributions in binary form must reproduce the above
    copyright notice, this list of conditions and the following
    disclaimer in the documentation and/or other materials provided
    with the distribution.

THIS SOFTWARE IS PROVIDED BY TOMAS PASTOREK AND CONTRIBUTORS ``AS IS'' 
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, 
THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR 
PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL TOMAS PASTOREK OR CONTRIBUTORS 
BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR 
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF 
SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS 
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN 
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) 
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF 
THE POSSIBILITY OF SUCH DAMAGE. 
 */

using System;

namespace NKafka
{
    internal class VarintBitConverter
    {
        public static unsafe string MakeString(byte* varint)
        {
            byte[] bs = new byte[varint[0]];
            for (int i=0; i<bs.Length; ++i)
            {
                bs[i] = varint[i+1];
            }
            return Utils.BytesToHexString(bs, bs.Length);
        }

        /// <summary>
        /// Returns the specified byte value as varint encoded array of bytes.   
        /// </summary>
        /// <param name="value">Byte value</param>
        /// <returns>Varint array of bytes.</returns>
        public static unsafe void GetVarintBytes(byte *b, byte value)
        {
            GetVarintBytes(b, (ulong)value);
        }

        /// <summary>
        /// Returns the specified 16-bit signed value as varint encoded array of bytes.   
        /// </summary>
        /// <param name="value">16-bit signed value</param>
        /// <returns>Varint array of bytes.</returns>
        public static unsafe void GetVarintBytes(byte *b, short value)
        {
            var zigzag = EncodeZigZag(value, 16);
            GetVarintBytes(b, (ulong)zigzag);
        }

        /// <summary>
        /// Returns the specified 16-bit unsigned value as varint encoded array of bytes.   
        /// </summary>
        /// <param name="value">16-bit unsigned value</param>
        /// <returns>Varint array of bytes.</returns>
        public static unsafe void GetVarintBytes(byte *b, ushort value)
        {
            GetVarintBytes(b, (ulong)value);
        }

        /// <summary>
        /// Returns the specified 32-bit signed value as varint encoded array of bytes.   
        /// </summary>
        /// <param name="value">32-bit signed value</param>
        /// <returns>Varint array of bytes.</returns>
        public static unsafe void GetVarintBytes(byte *b, int value)
        {
            var zigzag = EncodeZigZag(value, 32);
            GetVarintBytes(b, (ulong)zigzag);
        }

        /// <summary>
        /// Returns the specified 32-bit unsigned value as varint encoded array of bytes.   
        /// </summary>
        /// <param name="value">32-bit unsigned value</param>
        /// <returns>Varint array of bytes.</returns>
        public static unsafe void GetVarintBytes(byte *b, uint value)
        {
            GetVarintBytes(b, (ulong)value);
        }

        /// <summary>
        /// Returns the specified 64-bit signed value as varint encoded array of bytes.   
        /// </summary>
        /// <param name="value">64-bit signed value</param>
        /// <returns>Varint array of bytes.</returns>
        public static unsafe void GetVarintBytes(byte *b, long value)
        {
            var zigzag = EncodeZigZag(value, 64);
            GetVarintBytes(b, (ulong)zigzag);
        }

        /// <summary>
        ///     Returns the specified 64-bit unsigned value as varint encoded array of bytes.   
        /// </summary>
        /// <param name="value">
        ///     64-bit unsigned value
        /// </param>
        /// <returns>
        ///     Varint array of bytes.
        /// </returns>
        public static unsafe void GetVarintBytes(byte *b, ulong value)
        {
            byte* lengthPos = b;
            b += 1;
            do
            {
                var byteVal = value & 0x7f;
                value >>= 7;

                if (value != 0)
                {
                    byteVal |= 0x80;
                }

                *b++ = (byte)byteVal;

            } while (value != 0);

            *lengthPos = (byte)(b - lengthPos - 1); // put length in first byte.
        }

        /// <summary>
        /// Returns byte value from varint encoded array of bytes.
        /// </summary>
        /// <param name="bytes">Varint encoded array of bytes.</param>
        /// <returns>Byte value</returns>
        public static byte ToByte(byte[] bytes)
        {
            return (byte)ToTarget(bytes, 8);
        }

        /// <summary>
        ///     Returns 16-bit signed value from varint encoded array of bytes.
        /// </summary>
        /// <param name="bytes">
        ///     Varint encoded array of bytes.
        /// </param>
        /// <returns>
        ///     16-bit signed value
        /// </returns>
        public static short ToInt16(byte[] bytes)
        {
            var zigzag = ToTarget(bytes, 16);
            return (short)DecodeZigZag(zigzag);
        }

        /// <summary>
        /// Returns 16-bit usigned value from varint encoded array of bytes.
        /// </summary>
        /// <param name="bytes">Varint encoded array of bytes.</param>
        /// <returns>16-bit usigned value</returns>
        public static ushort ToUInt16(byte[] bytes)
        {
            return (ushort)ToTarget(bytes, 16);
        }

        /// <summary>
        /// Returns 32-bit signed value from varint encoded array of bytes.
        /// </summary>
        /// <param name="bytes">Varint encoded array of bytes.</param>
        /// <returns>32-bit signed value</returns>
        public static int ToInt32(byte[] bytes)
        {
            var zigzag = ToTarget(bytes, 32);
            return (int)DecodeZigZag(zigzag);
        }

        /// <summary>
        /// Returns 32-bit unsigned value from varint encoded array of bytes.
        /// </summary>
        /// <param name="bytes">Varint encoded array of bytes.</param>
        /// <returns>32-bit unsigned value</returns>
        public static uint ToUInt32(byte[] bytes)
        {
            return (uint)ToTarget(bytes, 32);
        }

        /// <summary>
        /// Returns 64-bit signed value from varint encoded array of bytes.
        /// </summary>
        /// <param name="bytes">Varint encoded array of bytes.</param>
        /// <returns>64-bit signed value</returns>
        public static long ToInt64(byte[] bytes)
        {
            var zigzag = ToTarget(bytes, 64);
            return DecodeZigZag(zigzag);
        }

        /// <summary>
        /// Returns 64-bit unsigned value from varint encoded array of bytes.
        /// </summary>
        /// <param name="bytes">Varint encoded array of bytes.</param>
        /// <returns>64-bit unsigned value</returns>
        public static ulong ToUInt64(byte[] bytes)
        {
            return ToTarget(bytes, 64);
        }

        private static long EncodeZigZag(long value, int bitLength)
        {
            return (value << 1) ^ (value >> (bitLength - 1));
        }

        private static long DecodeZigZag(ulong value)
        {
            if ((value & 0x1) == 0x1)
            {
                return (-1 * ((long)(value >> 1) + 1));
            }

            return (long)(value >> 1);
        }

        private static ulong ToTarget(byte[] bytes, int sizeBites)
        {
            int shift = 0;
            ulong result = 0;

            foreach (ulong byteValue in bytes)
            {
                ulong tmp = byteValue & 0x7f;
                result |= tmp << shift;

                if (shift > sizeBites)
                {
                    throw new ArgumentOutOfRangeException("bytes", "Byte array is too large.");
                }

                if ((byteValue & 0x80) != 0x80)
                {
                    return result;
                }

                shift += 7;
            }

            throw new ArgumentException("Cannot decode varint from byte array.", "bytes");
        }
    }
}