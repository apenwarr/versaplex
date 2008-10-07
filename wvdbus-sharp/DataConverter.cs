//
// Authors:
//   Miguel de Icaza (miguel@novell.com)
//
// See the following url for documentation:
//     http://www.mono-project.com/Mono_DataConvert
//     
// WARNING: This is a modified version!!!!  Most features were removed by
// Avery Pennarun <apenwarr@gmail.com>.
//
//
// Copyright (C) 2006 Novell, Inc (http://www.novell.com)
//
// Permission is hereby granted, free of charge, to any person obtaining
// a copy of this software and associated documentation files (the
// "Software"), to deal in the Software without restriction, including
// without limitation the rights to use, copy, modify, merge, publish,
// distribute, sublicense, and/or sell copies of the Software, and to
// permit persons to whom the Software is furnished to do so, subject to
// the following conditions:
// 
// The above copyright notice and this permission notice shall be
// included in all copies or substantial portions of the Software.
// 
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
// EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
// NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
// LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
// OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
// WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
//
using System;
using System.Collections;
using System.Text;

namespace Mono {

	unsafe internal abstract class DataConverter {

// Disables the warning: CLS compliance checking will not be performed on
//  `XXXX' because it is not visible from outside this assembly
#pragma warning disable  3019
		static DataConverter SwapConv = new SwapConverter ();
		static DataConverter CopyConv = new CopyConverter ();

		public static readonly bool IsLittleEndian = BitConverter.IsLittleEndian;
			
		public abstract double GetDouble (byte [] data, int index);
		public abstract float  GetFloat  (byte [] data, int index);
		public abstract long   GetInt64  (byte [] data, int index);
		public abstract int    GetInt32  (byte [] data, int index);

		public abstract short  GetInt16  (byte [] data, int index);

		public abstract uint   GetUInt32 (byte [] data, int index);
		public abstract ushort GetUInt16 (byte [] data, int index);
		public abstract ulong  GetUInt64 (byte [] data, int index);
		
		public abstract void PutBytes (byte [] dest, int destIdx, double value);
		public abstract void PutBytes (byte [] dest, int destIdx, float value);
		public abstract void PutBytes (byte [] dest, int destIdx, int value);
		public abstract void PutBytes (byte [] dest, int destIdx, long value);
		public abstract void PutBytes (byte [] dest, int destIdx, short value);

		public abstract void PutBytes (byte [] dest, int destIdx, ushort value);
		public abstract void PutBytes (byte [] dest, int destIdx, uint value);
		public abstract void PutBytes (byte [] dest, int destIdx, ulong value);

		public byte[] GetBytes (double value)
		{
			byte [] ret = new byte [8];
			PutBytes (ret, 0, value);
			return ret;
		}
		
		public byte[] GetBytes (float value)
		{
			byte [] ret = new byte [4];
			PutBytes (ret, 0, value);
			return ret;
		}
		
		public byte[] GetBytes (int value)
		{
			byte [] ret = new byte [4];
			PutBytes (ret, 0, value);
			return ret;
		}
		
		public byte[] GetBytes (long value)
		{
			byte [] ret = new byte [8];
			PutBytes (ret, 0, value);
			return ret;
		}
		
		public byte[] GetBytes (short value)
		{
			byte [] ret = new byte [2];
			PutBytes (ret, 0, value);
			return ret;
		}

		public byte[] GetBytes (ushort value)
		{
			byte [] ret = new byte [2];
			PutBytes (ret, 0, value);
			return ret;
		}
		
		public byte[] GetBytes (uint value)
		{
			byte [] ret = new byte [4];
			PutBytes (ret, 0, value);
			return ret;
		}
		
		public byte[] GetBytes (ulong value)
		{
			byte [] ret = new byte [8];
			PutBytes (ret, 0, value);
			return ret;
		}
		
		static public DataConverter LittleEndian {
			get {
				return BitConverter.IsLittleEndian ? CopyConv : SwapConv;
			}
		}

		static public DataConverter BigEndian {
			get {
				return BitConverter.IsLittleEndian ? SwapConv : CopyConv;
			}
		}

		static public DataConverter Native {
			get {
				return CopyConv;
			}
		}

		internal void Check (byte [] dest, int destIdx, int size)
		{
			if (dest == null)
				throw new ArgumentNullException ("dest");
			if (destIdx < 0 || destIdx > dest.Length - size)
				throw new ArgumentException ("destIdx");
		}
		
		class CopyConverter : DataConverter {
			public override double GetDouble (byte [] data, int index)
			{
				if (data == null)
					throw new ArgumentNullException ("data");
				if (data.Length - index < 8)
					throw new ArgumentException ("index");
				if (index < 0)
					throw new ArgumentException ("index");
				double ret;
				byte *b = (byte *)&ret;

				for (int i = 0; i < 8; i++)
					b [i] = data [index+i];

				return ret;
			}

			public override ulong GetUInt64 (byte [] data, int index)
			{
				if (data == null)
					throw new ArgumentNullException ("data");
				if (data.Length - index < 8)
					throw new ArgumentException ("index");
				if (index < 0)
					throw new ArgumentException ("index");

				ulong ret;
				byte *b = (byte *)&ret;

				for (int i = 0; i < 8; i++)
					b [i] = data [index+i];

				return ret;
			}

			public override long GetInt64 (byte [] data, int index)
			{
				if (data == null)
					throw new ArgumentNullException ("data");
				if (data.Length - index < 8)
					throw new ArgumentException ("index");
				if (index < 0)
					throw new ArgumentException ("index");

				long ret;
				byte *b = (byte *)&ret;

				for (int i = 0; i < 8; i++)
					b [i] = data [index+i];

				return ret;
			}
			
			public override float GetFloat  (byte [] data, int index)
			{
				if (data == null)
					throw new ArgumentNullException ("data");
				if (data.Length - index < 4)
					throw new ArgumentException ("index");
				if (index < 0)
					throw new ArgumentException ("index");

				float ret;
				byte *b = (byte *)&ret;

				for (int i = 0; i < 4; i++)
					b [i] = data [index+i];

				return ret;
			}
			
			public override int GetInt32  (byte [] data, int index)
			{
				if (data == null)
					throw new ArgumentNullException ("data");
				if (data.Length - index < 4)
					throw new ArgumentException ("index");
				if (index < 0)
					throw new ArgumentException ("index");

				int ret;
				byte *b = (byte *)&ret;

				for (int i = 0; i < 4; i++)
					b [i] = data [index+i];

				return ret;
			}
			
			public override uint GetUInt32 (byte [] data, int index)
			{
				if (data == null)
					throw new ArgumentNullException ("data");
				if (data.Length - index < 4)
					throw new ArgumentException ("index");
				if (index < 0)
					throw new ArgumentException ("index");

				uint ret;
				byte *b = (byte *)&ret;

				for (int i = 0; i < 4; i++)
					b [i] = data [index+i];

				return ret;
			}
			
			public override short GetInt16 (byte [] data, int index)
			{
				if (data == null)
					throw new ArgumentNullException ("data");
				if (data.Length - index < 2)
					throw new ArgumentException ("index");
				if (index < 0)
					throw new ArgumentException ("index");

				short ret;
				byte *b = (byte *)&ret;

				for (int i = 0; i < 2; i++)
					b [i] = data [index+i];

				return ret;
			}
			
			public override ushort GetUInt16 (byte [] data, int index)
			{
				if (data == null)
					throw new ArgumentNullException ("data");
				if (data.Length - index < 2)
					throw new ArgumentException ("index");
				if (index < 0)
					throw new ArgumentException ("index");

				ushort ret;
				byte *b = (byte *)&ret;

				for (int i = 0; i < 2; i++)
					b [i] = data [index+i];

				return ret;
			}
			
			public override void PutBytes (byte [] dest, int destIdx, double value)
			{
				Check (dest, destIdx, 8);
				fixed (byte *target = &dest [destIdx]){
					long *source = (long *) &value;

					*((long *)target) = *source;
				}
			}
			
			public override void PutBytes (byte [] dest, int destIdx, float value)
			{
				Check (dest, destIdx, 4);
				fixed (byte *target = &dest [destIdx]){
					uint *source = (uint *) &value;

					*((uint *)target) = *source;
				}
			}
			
			public override void PutBytes (byte [] dest, int destIdx, int value)
			{
				Check (dest, destIdx, 4);
				fixed (byte *target = &dest [destIdx]){
					uint *source = (uint *) &value;

					*((uint *)target) = *source;
				}
			}

			public override void PutBytes (byte [] dest, int destIdx, uint value)
			{
				Check (dest, destIdx, 4);
				fixed (byte *target = &dest [destIdx]){
					uint *source = (uint *) &value;

					*((uint *)target) = *source;
				}
			}
			
			public override void PutBytes (byte [] dest, int destIdx, long value)
			{
				Check (dest, destIdx, 8);
				fixed (byte *target = &dest [destIdx]){
					long *source = (long *) &value;

					*((long*)target) = *source;
				}
			}
			
			public override void PutBytes (byte [] dest, int destIdx, ulong value)
			{
				Check (dest, destIdx, 8);
				fixed (byte *target = &dest [destIdx]){
					ulong *source = (ulong *) &value;

					*((ulong *) target) = *source;
				}
			}
			
			public override void PutBytes (byte [] dest, int destIdx, short value)
			{
				Check (dest, destIdx, 2);
				fixed (byte *target = &dest [destIdx]){
					ushort *source = (ushort *) &value;

					*((ushort *)target) = *source;
				}
			}
			
			public override void PutBytes (byte [] dest, int destIdx, ushort value)
			{
				Check (dest, destIdx, 2);
				fixed (byte *target = &dest [destIdx]){
					ushort *source = (ushort *) &value;

					*((ushort *)target) = *source;
				}
			}
		}

		class SwapConverter : DataConverter {
			public override double GetDouble (byte [] data, int index)
			{
				if (data == null)
					throw new ArgumentNullException ("data");
				if (data.Length - index < 8)
					throw new ArgumentException ("index");
				if (index < 0)
					throw new ArgumentException ("index");

				double ret;
				byte *b = (byte *)&ret;

				for (int i = 0; i < 8; i++)
					b [7-i] = data [index+i];

				return ret;
			}

			public override ulong GetUInt64 (byte [] data, int index)
			{
				if (data == null)
					throw new ArgumentNullException ("data");
				if (data.Length - index < 8)
					throw new ArgumentException ("index");
				if (index < 0)
					throw new ArgumentException ("index");

				ulong ret;
				byte *b = (byte *)&ret;

				for (int i = 0; i < 8; i++)
					b [7-i] = data [index+i];

				return ret;
			}

			public override long GetInt64 (byte [] data, int index)
			{
				if (data == null)
					throw new ArgumentNullException ("data");
				if (data.Length - index < 8)
					throw new ArgumentException ("index");
				if (index < 0)
					throw new ArgumentException ("index");

				long ret;
				byte *b = (byte *)&ret;

				for (int i = 0; i < 8; i++)
					b [7-i] = data [index+i];

				return ret;
			}
			
			public override float GetFloat  (byte [] data, int index)
			{
				if (data == null)
					throw new ArgumentNullException ("data");
				if (data.Length - index < 4)
					throw new ArgumentException ("index");
				if (index < 0)
					throw new ArgumentException ("index");

				float ret;
				byte *b = (byte *)&ret;

				for (int i = 0; i < 4; i++)
					b [3-i] = data [index+i];

				return ret;
			}
			
			public override int GetInt32  (byte [] data, int index)
			{
				if (data == null)
					throw new ArgumentNullException ("data");
				if (data.Length - index < 4)
					throw new ArgumentException ("index");
				if (index < 0)
					throw new ArgumentException ("index");

				int ret;
				byte *b = (byte *)&ret;

				for (int i = 0; i < 4; i++)
					b [3-i] = data [index+i];

				return ret;
			}
			
			public override uint GetUInt32 (byte [] data, int index)
			{
				if (data == null)
					throw new ArgumentNullException ("data");
				if (data.Length - index < 4)
					throw new ArgumentException ("index");
				if (index < 0)
					throw new ArgumentException ("index");

				uint ret;
				byte *b = (byte *)&ret;

				for (int i = 0; i < 4; i++)
					b [3-i] = data [index+i];

				return ret;
			}
			
			public override short GetInt16 (byte [] data, int index)
			{
				if (data == null)
					throw new ArgumentNullException ("data");
				if (data.Length - index < 2)
					throw new ArgumentException ("index");
				if (index < 0)
					throw new ArgumentException ("index");

				short ret;
				byte *b = (byte *)&ret;

				for (int i = 0; i < 2; i++)
					b [1-i] = data [index+i];

				return ret;
			}
			
			public override ushort GetUInt16 (byte [] data, int index)
			{
				if (data == null)
					throw new ArgumentNullException ("data");
				if (data.Length - index < 2)
					throw new ArgumentException ("index");
				if (index < 0)
					throw new ArgumentException ("index");

				ushort ret;
				byte *b = (byte *)&ret;

				for (int i = 0; i < 2; i++)
					b [1-i] = data [index+i];

				return ret;
			}

			public override void PutBytes (byte [] dest, int destIdx, double value)
			{
				Check (dest, destIdx, 8);

				fixed (byte *target = &dest [destIdx]){
					byte *source = (byte *) &value;

					for (int i = 0; i < 8; i++)
						target [i] = source [7-i];
				}
			}
			
			public override void PutBytes (byte [] dest, int destIdx, float value)
			{
				Check (dest, destIdx, 4);

				fixed (byte *target = &dest [destIdx]){
					byte *source = (byte *) &value;

					for (int i = 0; i < 4; i++)
						target [i] = source [3-i];
				}
			}
			
			public override void PutBytes (byte [] dest, int destIdx, int value)
			{
				Check (dest, destIdx, 4);

				fixed (byte *target = &dest [destIdx]){
					byte *source = (byte *) &value;

					for (int i = 0; i < 4; i++)
						target [i] = source [3-i];
				}
			}
			
			public override void PutBytes (byte [] dest, int destIdx, uint value)
			{
				Check (dest, destIdx, 4);

				fixed (byte *target = &dest [destIdx]){
					byte *source = (byte *) &value;

					for (int i = 0; i < 4; i++)
						target [i] = source [3-i];
				}
			}
			
			public override void PutBytes (byte [] dest, int destIdx, long value)
			{
				Check (dest, destIdx, 8);

				fixed (byte *target = &dest [destIdx]){
					byte *source = (byte *) &value;

					for (int i = 0; i < 8; i++)
						target [i] = source [7-i];
				}
			}
			
			public override void PutBytes (byte [] dest, int destIdx, ulong value)
			{
				Check (dest, destIdx, 8);

				fixed (byte *target = &dest [destIdx]){
					byte *source = (byte *) &value;

					for (int i = 0; i < 4; i++)
						target [i] = source [7-i];
				}
			}
			
			public override void PutBytes (byte [] dest, int destIdx, short value)
			{
				Check (dest, destIdx, 2);

				fixed (byte *target = &dest [destIdx]){
					byte *source = (byte *) &value;

					for (int i = 0; i < 2; i++)
						target [i] = source [1-i];
				}
			}
			
			public override void PutBytes (byte [] dest, int destIdx, ushort value)
			{
				Check (dest, destIdx, 2);

				fixed (byte *target = &dest [destIdx]){
					byte *source = (byte *) &value;

					for (int i = 0; i < 2; i++)
						target [i] = source [1-i];
				}
			}
		}
		
	}
}
