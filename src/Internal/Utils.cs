// Copyright 2017 Matt Howlett, https://www.matthowlett.com
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Refer to LICENSE for more information.

namespace NKafka
{
    internal static class Utils
    {
        private static char ByteToHexChar(byte b)
        {
            if (b < 10)
            {
                return (char)(b + 48);
            }
            return (char)(b + 65 - 10);
        }

        public static string BytesToHexString(byte[] bs, int length)
        {
            var result = "";
            for (int i=0; i<length; ++i)
            {
                byte upper = (byte)((bs[i] & (byte)0xf0) >> 4);
                byte lower = (byte)(bs[i] & (byte)0x0f);
                result += ByteToHexChar(upper);
                result += ByteToHexChar(lower);
                if (i%4 == 3) 
                {
                    result += " ";
                }
            }
            return result;
        }
    }
}
