using System.Diagnostics;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using Voron.Util.Simd;

public unsafe class FastPForEncoder
{
    internal const int PrefixSizeBits = 10;

    private long* _entries;
    private byte* _entriesOutput;
    private long _entriesOutputCount = 0;
    private int _count;
    private int _blockNum;
    private ushort _sharedPrefix;
    private int _consumed;
    private List<uint>[] _exceptions = new List<uint>[32];
    private int[] _exceptionsStart = new int[32];
    private List<byte> _metadata = new();
    private int _metadataPos;

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
        _metadata.Clear();
        _metadataPos = 0;

        if (_entriesOutputCount < count)
        {
            var newCount = BitOperations.RoundUpToPowerOf2((uint)count);
            _entriesOutput = (byte*)Realloc(_entriesOutput, (int)newCount * sizeof(long));
            _entriesOutputCount = newCount;
        }
    }

    public (int Consumed, int SizeUsed) Encode()
    {
        _sharedPrefix = RemoveSharedPrefix();
        _consumed = DeltaEncode((long*)_entriesOutput, (uint*)_entriesOutput, _count);

        long totalSize = sizeof(uint) /*bitmap*/;
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
            totalSize += SimdBitPacking.RequireSizeSegmented(item.Count, j);
        }

        totalSize += _metadata.Count + sizeof(ushort);
        _metadataPos = 0;
        return (_consumed, (int)totalSize);
    }

    public (int Count, int SizeUsed) Write(byte* output, int outputSize)
    {
        Debug.Assert(outputSize <= ushort.MaxValue, "Output buffer too large, we use ushort for offsets and don't want to overflow");

        var sizeUsed = sizeof(PForHeader);
        if (sizeUsed > outputSize)
            return default;

        ref var header = ref Unsafe.AsRef<PForHeader>(output);

        header.SharedPrefix = _sharedPrefix;

        var baseline = _entries[Math.Max(0, _blockNum * 256 - 1)];
        if (header.SharedPrefix < (1 << PrefixSizeBits))
        {
            baseline >>= PrefixSizeBits;
        }

        header.Baseline = baseline;
        var exceptionsCounts = stackalloc int[33];
        var startingMetadataPosition = _metadataPos;

        var exceptionsRequiredSize = 0;
        var entriesOutput = (uint*)_entriesOutput;
        var oldBlockNum = _blockNum;
        while (_metadataPos < _metadata.Count)
        {
            var batchMetadataStart = _metadataPos;
            var numOfBits = _metadata[_metadataPos++];
            var numOfExceptions = _metadata[_metadataPos++];

            var reqSize = numOfBits * Vector256<byte>.Count;

            if (numOfExceptions > 0)
            {
                var maxNumOfBits = _metadata[_metadataPos++];
                var exceptionIndex = maxNumOfBits - numOfBits;
                var oldCount = exceptionsCounts[exceptionIndex];
                var newCount = oldCount + numOfExceptions;
                exceptionsCounts[exceptionIndex] = newCount;
                if (oldCount == 0)
                {
                    exceptionsRequiredSize += sizeof(ushort); // size for the number of items here
                }

                exceptionsRequiredSize -= SimdBitPacking.RequireSizeSegmented(oldCount, maxNumOfBits);
                exceptionsRequiredSize += SimdBitPacking.RequireSizeSegmented(newCount, maxNumOfBits);
            }

            _metadataPos += numOfExceptions;
            var metaSize = _metadataPos - startingMetadataPosition;

            var finalSize = (sizeUsed + reqSize + exceptionsRequiredSize + metaSize);
            if (finalSize > outputSize)
            {
                _metadataPos = batchMetadataStart;
                break;
            }
            SimdBitPacking<MaskEntries>.Pack256(0, entriesOutput + (_blockNum++ * 256),
                output + sizeUsed, numOfBits, new MaskEntries((1u << numOfBits) - 1));
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
                sizeUsed += BitPacking.PackSegmented(b, span.Length, output + sizeUsed, (uint)numOfBits);
            }
            _exceptionsStart[numOfBits] += count;
        }

        header.ExceptionsBitmap = bitmap;
        header.MetadataOffset = checked((ushort)sizeUsed);

        var metadataSize = (_metadataPos - startingMetadataPosition);
        var metadataSpan = CollectionsMarshal.AsSpan(_metadata);
        var metadataBlockRange = metadataSpan[startingMetadataPosition..(startingMetadataPosition + metadataSize)];
        metadataBlockRange.CopyTo(new Span<byte>(output + sizeUsed, metadataSize));

        sizeUsed += metadataSize;

        return ((_blockNum - oldBlockNum) * 256, sizeUsed);
    }

    internal static void* Realloc(void* ptr, int size) => (byte*)Marshal.ReAllocHGlobal((nint)ptr, size);

    private int ProcessBlock(uint* currentEntries)
    {
        var (bestB, maxB, exceptionCount) = FindBestBitWidths(currentEntries);
        _metadata.Add((byte)bestB);
        _metadata.Add((byte)exceptionCount);
        if (exceptionCount > 0)
        {
            _metadata.Add((byte)maxB);
        }

        uint maxVal = 1u << bestB;
        ref var exceptionBuffer = ref _exceptions[maxB - bestB];

        for (int j = 0; j < 256; j++)
        {
            if (currentEntries[j] >= maxVal)
            {
                var exList = _exceptions[maxB - bestB] ??= new();
                exList.Add(currentEntries[j] >>> bestB);
                _metadata.Add((byte)j);
            }
        }

        return bestB * Vector256<byte>.Count;
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