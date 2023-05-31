using System.Data;
using System.Diagnostics;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Windows.Markup;
using Voron.Util.Simd;

var list = File.ReadLines(@"C:\Users\ayende\Downloads\976504836.txt").Select(long.Parse).ToArray();
unsafe
{
    fixed (long* l = list)
    {
        var fpor = new FastPFor();
        fpor.Init(l, list.Length);

        Console.WriteLine(fpor.Encode());

        var b = new byte[1024];

        var output = new long[1024];
        fixed (long* o = output)
        fixed (byte* p = b)
        {
            var a = fpor.Write(p, 1024);

            var decoder = new FastPFor.Decoder(p, a.SizeUsed);
            var read = decoder.Read(o, 1024);
            Console.WriteLine(list.Take(1024).SequenceEqual(output));
        }

    }
}

public unsafe class FastPFor
{
    public unsafe struct Decoder
    {
        private byte* _input;
        private byte* _metadata;
        private readonly byte* _end;
        private readonly uint* _exceptions;
        private long _baseline;
        private fixed int _exceptionOffsets[32];
        private readonly int _prefixShiftAmount;
        private readonly ushort _sharedPrefix;

        public Decoder(byte* input, int size)
        {
            _input = input;
            _end = input + size;
            ref var header = ref Unsafe.AsRef<Header>(input);
            _metadata = input + header.MetadataOffset;
            _baseline = header.Baseline;
            if (header.SharedPrefix >= 1 << PrefixSizeBits)
            {
                _sharedPrefix = 0;
                _prefixShiftAmount = 0;
            }
            else
            {
                _prefixShiftAmount = PrefixSizeBits;
                _sharedPrefix = header.SharedPrefix;
            }

            _exceptions = null;
            var exceptionsBufferSize = 0;
            var exceptionBufferOffset = 0;

            var exception = _input + header.ExceptionsOffset;
            for (int i = 2; i <= 32; i++)
            {
                if ((header.ExceptionsBitmap & (1 << (i - 1))) == 0)
                    continue;

                var count = Unsafe.Read<ushort>(exception);
                exception += sizeof(ushort);

                if (count + exceptionBufferOffset > exceptionsBufferSize)
                {
                    exceptionsBufferSize = Math.Max(exceptionsBufferSize * 2, 1024);
                    _exceptions = (uint*)Realloc(_exceptions, exceptionsBufferSize * sizeof(uint));
                }

                SimdCompression<NoTransform>.UnpackSegmented(0, exception, count, _exceptions + exceptionBufferOffset, (uint)i);
                _exceptionOffsets[i] = exceptionBufferOffset;
                exceptionBufferOffset += count;

                exception += SimdCompression.RequiredBufferSize(count, i);
            }

            _input += sizeof(Header);

        }

        public int Read(long* output, int outputCount)
        {
            var buffer = stackalloc uint[256];
            int read = 0;
            while (_metadata < _end && read < outputCount)
            {
                var numOfBits = *_metadata++;
                var numOfExceptions = *_metadata++;

                SimdCompression<NoTransform>.Unpack256(0, _input, buffer, numOfBits);
                _input += SimdCompression.RequiredBufferSize(256, numOfBits);

                if (numOfExceptions == 0)
                    continue;

                var maxNumOfBits = *_metadata++;
                var bitsDiff = maxNumOfBits - numOfBits;
                if (bitsDiff == 1)
                {
                    var mask = 1u << numOfBits;
                    for (int i = 0; i < numOfExceptions; i++)
                    {
                        var idx = *_metadata++;
                        buffer[idx] |= mask;
                    }
                }
                else
                {
                    ref var offset = ref _exceptionOffsets[bitsDiff];
                    for (int i = 0; i < numOfExceptions; i++)
                    {
                        var remains = _exceptions[offset++];
                        var idx = *_metadata++;
                        buffer[idx] |= remains << numOfBits;
                    }
                }


                for (int i = 0; i + Vector256<uint>.Count <= 256; i += Vector256<uint>.Count)
                {
                    var (a, b) = Vector256.Widen(Vector256.Load(buffer + i).AsInt32());
                    a.Store(output + read);
                    read += Vector256<ulong>.Count;
                    b.Store(output + read);
                    read += Vector256<ulong>.Count;
                }

                var sharedPrefixMask = Vector256.Create<long>(_sharedPrefix);
                var prev = Vector256.Create(_baseline);
                for (int i = 0; i + Vector256<long>.Count <= 256; i += Vector256<long>.Count)
                {
                    var cur = Vector256.Load(output + i);
                    cur += Vector256.Shuffle(cur, Vector256.Create(0, 0, 1, 2)) &
                            Vector256.Create(0, -1, -1, -1);
                    cur += Vector256.Shuffle(cur, Vector256.Create(0, 0, 0, 1)) &
                            Vector256.Create(0, 0, -1, -1);
                    cur += prev;
                    prev = Vector256.Shuffle(cur, Vector256.Create(3, 3, 3, 3));
                    cur = Vector256.ShiftLeft(cur, _prefixShiftAmount) | sharedPrefixMask;
                    cur.Store(output + i);
                }
            }

            return read;
        }
    }


    public struct Header
    {
        public long Baseline;
        public ushort MetadataOffset;
        public ushort ExceptionsOffset;
        public uint ExceptionsBitmap;
        public ushort SharedPrefix;
    }

    const int PrefixSizeBits = 10;

    private long* _entries;
    private byte* _entriesOutput;
    private long _entriesOutputCount = 0;
    private int _count;
    private int _blockNum;
    private ushort _sharedPrefix;
    private int _consumed;
    private List<uint>[] _exceptions = new List<uint>[32];
    private int[] _exceptionsStart = new int[32];
    private MemoryStream _metadata = new MemoryStream();

    public void Init(long* entries, int count)
    {
        _entries = entries;
        _count = count;
        _blockNum = 0;
        for (int i = 0; i < _exceptions.Length; i++)
        {
            _exceptions[i]?.Clear();
            _exceptionsStart[i] = 0;
        }
        _metadata.SetLength(0);

        if (_entriesOutputCount < count)
        {
            var s = BitOperations.RoundUpToPowerOf2((uint)count);
            _entriesOutput = (byte*)Realloc(_entriesOutput, (int)s);
            //TODO: free me!
            _entriesOutputCount = s;
        }
    }

    public (int Consumed, int SizeUsed) Encode()
    {
        _sharedPrefix = RemoveSharedPrefix();
        _consumed = DeltaEncode((long*)_entriesOutput, (uint*)_entriesOutput, _count);

        long totalSize = 0;
        int i = 0;
        var entriesAsInt = (uint*)_entriesOutput;
        for (; i + 256 <= _consumed; i += 256)
        {
            totalSize += ProcessBlock(entriesAsInt + i);
        }

        for (int j = 2; j < _exceptions.Length; j++)
        {
            var item = _exceptions[j];
            if (item == null || item.Count == 0)
                continue;
            totalSize += SimdCompression.RequiredBufferSize(item.Count, j);
        }

        totalSize += _metadata.Length + sizeof(ushort);
        _metadata.Position = 0;
        return (_consumed, (int)totalSize);
    }

    public (int Count, int SizeUsed) Write(byte* output, int outputSize)
    {
        Debug.Assert(outputSize <= ushort.MaxValue, "Output buffer too large, we use ushort for offsets and don't want to overflow");

        var sizeUsed = sizeof(Header);
        if (sizeUsed > outputSize)
            return default;

        ref var header = ref Unsafe.AsRef<Header>(output);

        header.SharedPrefix = _sharedPrefix;

        var baseline = _entries[Math.Max(0, _blockNum * 256 - 1)];
        if (header.SharedPrefix < (1 << PrefixSizeBits))
        {
            baseline >>= PrefixSizeBits;
        }

        header.Baseline = baseline;
        var exceptionsCounts = stackalloc int[33];
        var startingMetadataPosition = _metadata.Position;

        var exceptionsRequiredSize = 0;
        var entriesOutput = (uint*)_entriesOutput;
        var oldBlockNum = _blockNum;
        while (true)
        {
            var batchMetadataStart = _metadata.Position;
            var numOfBits = _metadata.ReadByte();
            if (numOfBits == -1)
                break;
            var numOfExceptions = _metadata.ReadByte();

            var reqSize = SimdCompression.RequiredBufferSize(256, numOfBits);

            if (numOfExceptions > 0)
            {
                var maxNumOfBits = _metadata.ReadByte();
                var exceptionIndex = maxNumOfBits - numOfBits;
                var oldCount = exceptionsCounts[exceptionIndex];
                var newCount = oldCount + numOfExceptions;
                exceptionsCounts[exceptionIndex] = newCount;
                if (oldCount == 0)
                {
                    exceptionsRequiredSize += sizeof(ushort); // size for the number of items here
                }

                exceptionsRequiredSize -= SimdCompression.RequireSizeSegmented(oldCount, maxNumOfBits);
                exceptionsRequiredSize += SimdCompression.RequireSizeSegmented(newCount, maxNumOfBits);
            }

            _metadata.Seek(numOfExceptions, SeekOrigin.Current);

            var metaSize = _metadata.Position - startingMetadataPosition;

            var finalSize = (sizeUsed + reqSize + exceptionsRequiredSize + metaSize);
            if (finalSize > outputSize)
            {
                _metadata.Position = batchMetadataStart;
                break;
            }
            SimdCompression<MaskEntries>.Pack256(0, entriesOutput + (_blockNum++ * 256), output + sizeUsed, (uint)numOfBits,
                new MaskEntries((1u << numOfBits) - 1));
            sizeUsed += reqSize;
        }

        uint bitmap = 0;
        header.ExceptionsOffset = checked((ushort)sizeUsed);

        for (int numOfBits = 2; numOfBits <= 32; numOfBits++)
        {
            var count = exceptionsCounts[numOfBits];
            if (count == 0)
                continue;

            bitmap |= 1u << numOfBits - 1;
            Unsafe.Write(output + sizeUsed, (ushort)count);
            sizeUsed += sizeof(ushort);
            var span = CollectionsMarshal.AsSpan(_exceptions[numOfBits]);
            var exceptionStart = _exceptionsStart[numOfBits];
            span = span[exceptionStart..(exceptionStart + count)];
            fixed (uint* b = span)
            {
                sizeUsed += SimdCompression<NoTransform>.PackSegmented(0, b, span.Length, output + sizeUsed, (uint)numOfBits);
            }
            _exceptionsStart[numOfBits] += count;
        }

        header.ExceptionsBitmap = bitmap;
        header.MetadataOffset = checked((ushort)sizeUsed);

        var metadataSize = (int)(_metadata.Position - startingMetadataPosition);

        _metadata.Position = startingMetadataPosition;

        _metadata.ReadExactly(new Span<byte>(output + sizeUsed, metadataSize));

        sizeUsed += metadataSize;

        return ((_blockNum - oldBlockNum) * 256, sizeUsed);
    }


    private static byte* Allocate(uint size) => (byte*)Marshal.AllocHGlobal((int)size);
    private static void* Realloc(void* ptr, int size) => (byte*)Marshal.ReAllocHGlobal((nint)ptr, size);

    private int ProcessBlock(uint* currentEntries)
    {
        var (bestB, maxB, exceptionCount) = FindBestBitWidths(currentEntries);
        _metadata.WriteByte((byte)bestB);
        _metadata.WriteByte((byte)exceptionCount);
        if (exceptionCount > 0)
        {
            _metadata.WriteByte((byte)maxB);
        }

        uint maxVal = 1u << bestB;
        ref var exceptionBuffer = ref _exceptions[maxB - bestB];

        for (int j = 0; j < 256; j++)
        {
            if (currentEntries[j] >= maxVal)
            {
                var exList = _exceptions[maxB - bestB] ??= new();
                exList.Add(currentEntries[j] >>> bestB);
                _metadata.WriteByte((byte)j);
            }
        }

        return SimdCompression.RequiredBufferSize(256, bestB);
    }

    private static (int BestBitWidth, int MaxBitWidth, int ExceptionsCount) FindBestBitWidths(uint* entries)
    {
        const int blockSize = 256;
        const int exceptionOverhead = 8;

        var freqs = stackalloc int[33];
        for (int i = 0; i < blockSize; i++)
        {
            freqs[32 - BitOperations.LeadingZeroCount(entries[i])]++;
        }
        var bestBitWidth = 32;
        while (freqs[bestBitWidth] == 0 && bestBitWidth > 0)
        {
            bestBitWidth--;
        }
        var maxBitWidth = bestBitWidth;
        var bestCost = bestBitWidth * blockSize;
        var bestExceptionCount = 0;
        var exceptionsCount = 0;
        for (var curBitWidth = bestBitWidth - 1; curBitWidth >= 0; curBitWidth--)
        {
            exceptionsCount += freqs[curBitWidth + 1];
            // TODO: figure out the actual cost here
            var curCost = exceptionsCount * exceptionOverhead +
                exceptionsCount * (maxBitWidth - curBitWidth) +
                curBitWidth * blockSize
                + 8;

            if (curCost < bestCost)
            {
                bestCost = curCost;
                bestBitWidth = curBitWidth;
                bestExceptionCount = exceptionsCount;
            }
        }

        return (bestBitWidth, maxBitWidth, bestExceptionCount);
    }

    private static int DeltaEncode(long* entries, uint* entriesOutput, int count)
    {
        int i = 0;
        var prev = Vector256.Create(*entries);
        var max = Vector256.Create<long>(uint.MaxValue);
        for (; i + Vector256<long>.Count <= count; i += Vector256<long>.Count)
        {
            var cur = Vector256.Load(entries + i);
            var mixed = Vector256.Shuffle(cur, Vector256.Create(0, 0, 1, 2)) & Vector256.Create(0, -1, -1, -1) |
                        Vector256.Shuffle(prev, Vector256.Create(3, 3, 3, 3)) & Vector256.Create(-1, 0, 0, 0);
            prev = cur;
            var delta = cur - mixed;
            var deltaInts = Vector256.Narrow(delta.AsUInt64(), Vector256<ulong>.Zero);

            if (Vector256.GreaterThanAny(delta, max))
                throw new NotSupportedException("think how to resume this");
            //return i & ~255; // align to 256 boundary down

            deltaInts.Store(entriesOutput);
            // we write 8 values, but increment by 4, so we'll overwrite it next op
            entriesOutput += Vector256<long>.Count;
        }
        return i;
    }

    private ushort RemoveSharedPrefix()
    {

        var maskScalar = (1L << PrefixSizeBits) - 1;
        var mask = Vector256.Create<long>(maskScalar);
        var prefixScalar = *_entries & maskScalar;
        var prefix = Vector256.Create(prefixScalar);
        int i = 0;
        var output = (long*)_entriesOutput;
        for (; i + Vector256<long>.Count <= _count; i += Vector256<long>.Count)
        {
            var cur = Vector256.Load(_entries + i);
            if ((cur & mask) != prefix)
            {
                return NoSharedPrefix();
            }
            Vector256.ShiftRightLogical(cur, PrefixSizeBits).Store(output + i);
        }

        for (; i < _count; i++)
        {
            var cur = _entries[i];
            if ((cur & maskScalar) != prefixScalar)
            {
                return NoSharedPrefix();
            }
            output[i] = cur >>> PrefixSizeBits;
        }

        return (ushort)prefixScalar;

        ushort NoSharedPrefix()
        {
            Unsafe.CopyBlock(_entriesOutput, _entriesOutput, (uint)(sizeof(long) * _count));
            return ushort.MaxValue; // invalid value, since > 10 bits
        }

    }

    private static int WriteRunLengthTuple(byte* buffer, int val, int len)
    {
        int i = 0;
        Write(val);
        Write(len);
        return i;

        void Write(int v)
        {
            while (v > 0x7F)
            {
                buffer[i++] = (byte)(v | ~0x7F);
                v >>>= 7;
            }
            buffer[i++] = (byte)v;
        }
    }
}