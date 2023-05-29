using System;
using System.Data;
using System.Diagnostics;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Threading.Tasks;
using Voron.Util.Simd;

var list = File.ReadLines(@"f:/postingList2.txt").Select(long.Parse).ToArray();
unsafe
{
    fixed (long* l = list)
    {
        var fpor = new FastPFor();
        fpor.Init(l, list.Length);

        Console.WriteLine(fpor.Encode());

        var b = new byte[1024];

        fixed(byte* p = b)
        {
            var a = fpor.Write(p, 1024);
            Console.WriteLine(a);
        }

    }
}

public unsafe struct FastPForDecoder
{
    private byte* _input;
    private byte* _metadata;
    private readonly byte* _end;
    private readonly uint _exceptionsBitmap;
    private uint _initValue;
    private readonly ushort _sharedPrefix;
    

    public FastPForDecoder(byte* input, int size)
    {
        _input = input;
        _end = input + size;

        _sharedPrefix = Unsafe.Read<ushort>(input);
        _exceptionsBitmap = Unsafe.Read<uint>(input + sizeof(ushort));
        var metadataOffset = Unsafe.Read<uint>(input + sizeof(ushort) + sizeof(uint));
        _metadata = metadataOffset + _input;
        _input += sizeof(ushort) + sizeof(uint) + sizeof(uint);
        _initValue = 0;
    }

    public int Read(long*output, int outputCount)
    {
        var buffer = stackalloc uint[256];
        while (_metadata < _end)
        {
            var numOfBits = *_metadata++;
            var numOfExceptions = *_metadata++;

            SimdCompression<NoTransform>.Unpack256(_initValue, _input, buffer, numOfBits);
            _input += SimdCompression.RequiredBufferSize(256, numOfBits);

        }


        return 0;
    }
}

public unsafe class FastPFor
{
    private long* _entries;
    private byte* _entriesOutput;
    private long _entriesOutputCount = 0;
    private int _count;
    private int _blockNum;
    private ushort _sharedPrefix;
    private int _consumed;
    private List<uint>[] _exceptions = new List<uint>[32];
    private MemoryStream _metadata = new MemoryStream();

    public void Init(long* entries, int count)
    {
        _entries = entries;
        _count = count;
        _blockNum = 0;
        for (int i = 0; i < _exceptions.Length; i++)
        {
            _exceptions[i]?.Clear();
        }
        _metadata.SetLength(0);

        if (_entriesOutputCount < count)
        {
            var s = BitOperations.RoundUpToPowerOf2((uint)count);
            _entriesOutput = Allocate(sizeof(long) * s);
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
            List<uint>? item = _exceptions[j];
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
        var sizeUsed = sizeof(short) /*shared prefix*/ + sizeof(uint) /*bitmap*/ + sizeof(int) /* metadata offset */;
        if (sizeUsed > outputSize)
            return default;

        Unsafe.Write(output, _sharedPrefix);
        ref var exceptionsBitmap = ref Unsafe.AsRef<uint>(output + sizeof(ushort));
        ref var offsetToMetadata = ref Unsafe.AsRef<uint>(output + sizeof(ushort) + sizeof(uint));

        var exceptionsCounts = stackalloc int[33];

        var startingMetadataPosition = _metadata.Position;

        var exceptionsRequiredSize = 0;
        var entriesOutput = (uint*)_entriesOutput;
        uint start = entriesOutput[0];
        var oldBlockNum = _blockNum;
        while (true)
        {
            var numOfBits = _metadata.ReadByte();
            if (numOfBits ==-1)
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

                exceptionsRequiredSize -= SimdCompression.RequiredBufferSize(oldCount, maxNumOfBits);
                exceptionsRequiredSize += SimdCompression.RequiredBufferSize(newCount, maxNumOfBits);
            }

            _metadata.Seek(numOfExceptions, SeekOrigin.Current);

            var metaSize = _metadata.Position - startingMetadataPosition;

            var finalSize = (sizeUsed + reqSize + exceptionsRequiredSize + metaSize);
            if (finalSize > outputSize)
                break;

            SimdCompression<MaskEntries>.Pack256(start, entriesOutput + (_blockNum++ * 256), output + sizeUsed, (uint)numOfBits,
                new MaskEntries((1u << numOfBits) - 1));
            sizeUsed += reqSize;
            start = entriesOutput[255];
        }

        uint bitmap = 0;
        for (int numOfBits = 2; numOfBits <= 32; numOfBits++)
        {
            if (exceptionsCounts[numOfBits] == 0)
                continue;

            bitmap |= 1u << numOfBits;
            var exceptionsList = _exceptions[numOfBits];
            var rangeToWrite = CollectionsMarshal.AsSpan(exceptionsList)[0..exceptionsCounts[numOfBits]];
            fixed (uint* b = rangeToWrite)
            {
                sizeUsed += SimdCompression<NoTransform>.PackSmall(0, b, exceptionsCounts[numOfBits],
                    output + sizeUsed, (uint)numOfBits);
            }
            exceptionsList.RemoveRange(0, exceptionsCounts[numOfBits]);
        }

        exceptionsBitmap = bitmap;
        offsetToMetadata = (uint)sizeUsed;

        var metadataSize = (int)(_metadata.Position - startingMetadataPosition);

        _metadata.Position = startingMetadataPosition;

        _metadata.ReadExactly(new Span<byte>(output + sizeUsed, metadataSize));

        sizeUsed += metadataSize;

        return ((_blockNum - oldBlockNum) * 256, sizeUsed);
    }


    private static byte* Allocate(uint size) => (byte*)Marshal.AllocHGlobal((int)size);

    //public static bool Encode(long* entries, int count, byte* output, int outputSize)
    //{
    //    var metadata = new MemoryStream();
    //    var exceptionsContainers = new List<ulong>[65];
    //    var rle = new BinaryWriter(new MemoryStream());

    //    var entriesOutput = (long*)Allocate(count * sizeof(long));
    //    var entriesOutputAsInt = (uint*)entriesOutput;
    //    RunLengthEncodePrefixes(entries, entriesOutput, count, rle);

    //    var requiredSize = rle.BaseStream.Length;

    //    var consumed = DeltaEncode(entriesOutput, count);

    //    int i = 0;
    //    for (; i + 256 <= consumed; i += 256)
    //    {
    //        ProcessBlock(metadata, exceptionsContainers, entriesOutputAsInt, i);
    //    }
    //    ulong bitmap = 0;
    //    // zero is never a valid value, and 1 is always going to be constant, so we can assume it and not store
    //    for (int j = 2; j <= 32; j++)
    //    {
    //        if (exceptionsContainers[j] != null)
    //        {
    //            bitmap |= 1u << (j - 1);
    //        }
    //        //  pack(exceptionsContainers[i]);
    //    }

    //    return true;
    //}

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
        if (_exceptions[maxB - bestB] == null)
        {
            _exceptions[maxB - bestB] = new List<uint>();
        }
        var exceptions = _exceptions[maxB - bestB];
        for (int j = 0; j < 256; j++)
        {
            if (currentEntries[j] >= maxVal)
            {
                exceptions.Add(currentEntries[j] >>> bestB);
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
        const int prefixSizeBits = 10;

        var maskScalar = (1L << prefixSizeBits) - 1;
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
            Vector256.ShiftRightLogical(cur, prefixSizeBits).Store(output + i);
        }

        for (; i < _count; i++)
        {
            var cur = _entries[i];
            if ((cur & maskScalar) != prefixScalar)
            {
                return NoSharedPrefix();
            }
            output[i] = cur >>> prefixSizeBits;
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