﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;

namespace SimdPFor;

public unsafe class FastPForEncoder  : IDisposable
{
    public const byte VarIntBatchMarker = 255;
    public const byte BiggerThanMaxMarker = 254;
    
    private long* _entries;
    private byte* _entriesOutput;
    private long _entriesOutputSize;
    private int _count;
    private int _offset;
    private readonly List<uint>[] _exceptions = new List<uint>[33];
    private readonly int[] _exceptionsStart = new int[33];
    private readonly List<byte> _metadata = new();
    private int _metadataPos;

    [Conditional("DEBUG")]
    private static void AssertIsSorted(long* entries, int count)
    {
        if (count <= 1)
        {
            // If there are 0 or 1 elements, it is considered sorted
            return;
        }

        for (int i = 0; i < count - 1; i++)
        {
            Debug.Assert(entries[i] >= 0);
            if (entries[i] > entries[i + 1])
            {
                throw new InvalidOperationException("The entries are not sorted.");
            }
        }
    }    
    public int Encode(long* entries, int count)
    {
        AssertIsSorted(entries, count);
        _entries = entries;
        _count = count;
        _offset = 0;
        _metadataPos = 0;
        
        for (int k = 0; k < _exceptions.Length; k++)
        {
            _exceptions[k]?.Clear();
            _exceptionsStart[k] = 0;
        }
        _metadata.Clear();
        
        if (_entriesOutputSize < count)
        {
            var newCount = Math.Max(256, BitOperations.RoundUpToPowerOf2((uint)count));
            _entriesOutput = (byte*)NativeMemory.Realloc(_entriesOutput, newCount * sizeof(long));
            _entriesOutputSize = newCount;
        }

        var totalSize = sizeof(PForHeader);
        int i = 0;
        var entriesAsInt = (uint*)_entriesOutput;
        var entriesIn = _entries;
        var prev = Vector256.Create(*entriesIn);
        var max = Vector256.Create<long>(uint.MaxValue);
        for (; i + 256 <= _count; i += 256)
        {
            var blockStart = entriesAsInt;
            int j = 0;
            for (; j < 256; j += Vector256<long>.Count)
            {
                var cur = Vector256.Load(entriesIn + i + j);
                var mixed = Vector256.Shuffle(cur, Vector256.Create(0, 0, 1, 2)) & Vector256.Create(0, -1, -1, -1) |
                            Vector256.Shuffle(prev, Vector256.Create(3, 3, 3, 3)) & Vector256.Create(-1, 0, 0, 0);
                prev = cur;
                var delta = cur - mixed;

                if (Vector256.GreaterThanAny(delta, max))
                {
                    HandleDeltaGreaterThanMax(j, delta);
                }

                var deltaInts = Vector256.Shuffle(delta.AsUInt32(), Vector256.Create(0u,2,4,6,0,0,0,0));
                deltaInts.Store(entriesAsInt);
                // we write 8 values, but increment by 4, so we'll overwrite it next op
                entriesAsInt += Vector256<long>.Count;
            }
            totalSize += ProcessBlock(blockStart);
        }
        if (i < _count)
        {
            var remainder = _count - i;
            Debug.Assert(remainder is > 0 and < 256);
            _metadata.Add(VarIntBatchMarker);
            _metadata.Add((byte)remainder); 
            var prevIdx = Math.Max(0, i - 1);
            totalSize += ComputeVarIntDeltaSize(entriesIn[prevIdx], entriesIn + i, remainder);
        }

        for (int j = 2; j < _exceptions.Length; j++)
        {
            var item = _exceptions[j];
            if (item == null || item.Count == 0)
                continue;
            int exceptionSize = BitPacking.RequireSizeSegmented(item.Count, j);
            totalSize += sizeof(ushort);// size of the exception
            totalSize += exceptionSize;
        }

        totalSize += _metadata.Count;
        _metadataPos = 0;
        return totalSize;
        
        void HandleDeltaGreaterThanMax(int batchOffset, Vector256<long> delta)
        {
            // we expect this to be *rare*, so not trying to optimize this in any special way
            Span<uint> deltaHighBuffer = stackalloc uint[8];
            var highAndLow = Vector256.Shuffle(delta.AsUInt32(), Vector256.Create(1u, 3, 5, 7, 0, 2, 4, 6));
            highAndLow.StoreUnsafe(ref deltaHighBuffer[0]);

            _metadata.Add(BiggerThanMaxMarker);
            _metadata.Add((byte)batchOffset);
            Span<byte> asBytes = MemoryMarshal.AsBytes(deltaHighBuffer);
            for (int j = 0; j < 16; j++)
            {
                _metadata.Add(asBytes[j]);
            }
        }
    }

    private static int ComputeVarIntDeltaSize(long previous, long* buffer, int count)
    {
        var size = 0;
        for (int i = 0; i < count; i++)
        {
            var cur = buffer[i] ;
            ulong num = (ulong)(cur - previous) | 1;
            int sizeVarInt = (64 - BitOperations.LeadingZeroCount(num) + 6) / 7;
            size += sizeVarInt;
            previous = cur;
        }
        return size;
    }

    public bool Done => _metadataPos == _metadata.Count;

    public (int Count, int SizeUsed) Write(byte* output, int outputSize)
    {
        Debug.Assert(outputSize <= ushort.MaxValue, "Output buffer too large, we use ushort for offsets and don't want to overflow");

        var sizeUsed = sizeof(PForHeader);
        if (sizeUsed > outputSize)
            return default;

        ref var header = ref Unsafe.AsRef<PForHeader>(output);

        var baseline = _entries[Math.Max(0, _offset - 1)];

        header.Baseline = baseline;
        var exceptionsCounts = stackalloc int[33];
        var startingMetadataPosition = _metadataPos;

        var exceptionsRequiredSize = 0;
        var entriesOutput = (uint*)_entriesOutput;
        var oldOffset = _offset;
        while (_metadataPos < _metadata.Count)
        {
            var batchMetadataStart = _metadataPos;
            var numOfBits = _metadata[_metadataPos++];

            switch (numOfBits)
            {
                case BiggerThanMaxMarker:
                    // nothing to do, doesn't impact writes 
                    _metadataPos += 17; // batch offset + 16 bytes
                    continue;
                case VarIntBatchMarker:
                    var sizeOfVarIntBatch = _metadata[_metadataPos++];
                    var varintSize = WriteLastBatchAsVarIntDelta(sizeOfVarIntBatch, output + sizeUsed, outputSize - sizeUsed);
                    var expectedMetadataSize = _metadataPos - startingMetadataPosition;
                    if (varintSize == 0 || // couldn't fit the var int buffer
                        sizeUsed + varintSize + exceptionsRequiredSize + expectedMetadataSize > outputSize) // wouldn't be able to fit the exceptions & metadata
                    {
                        _metadataPos = batchMetadataStart;
                        goto AfterLoop;
                    }

                    _offset += sizeOfVarIntBatch;
                    sizeUsed += varintSize;
                    continue;
                case > 32 and < BiggerThanMaxMarker:
                    throw new ArgumentOutOfRangeException("Invalid bits value: " + numOfBits);
            }

            var numOfExceptions = _metadata[_metadataPos++];

            var reqSize = numOfBits * Vector256<byte>.Count;

            ref var exceptionCountRef = ref exceptionsCounts[0];
            var amountToAddToException = 0;
            
            if (numOfExceptions > 0)
            {
                var maxNumOfBits = _metadata[_metadataPos++];
                var exceptionIndex = maxNumOfBits - numOfBits;
                if (exceptionIndex > 1)
                {
                    var oldCount = exceptionsCounts[exceptionIndex];
                    amountToAddToException = numOfExceptions;
                    exceptionCountRef = ref exceptionsCounts[exceptionIndex];
                
                    if (oldCount == 0)
                    {
                        exceptionsRequiredSize += sizeof(ushort); // size for the number of items here
                    }
                    exceptionsRequiredSize -= BitPacking.RequireSizeSegmented(oldCount, exceptionIndex);
                    exceptionsRequiredSize += BitPacking.RequireSizeSegmented(numOfExceptions + oldCount, exceptionIndex);
                }
            }

            _metadataPos += numOfExceptions;
            var metaSize = _metadataPos - startingMetadataPosition;

            var finalSize = (sizeUsed + reqSize + exceptionsRequiredSize + metaSize);
            if (finalSize > outputSize)
            {
                _metadataPos = batchMetadataStart;
                break;
            }
            SimdBitPacking<MaskEntries>.Pack256(0, entriesOutput + _offset,
                output + sizeUsed, numOfBits, new MaskEntries((1u << numOfBits) - 1));
            sizeUsed += reqSize;
            _offset += 256;
            exceptionCountRef += amountToAddToException;
        }
        AfterLoop:

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
                int size = BitPacking.PackSegmented(b, span.Length, output + sizeUsed, (uint)numOfBits);
                sizeUsed += size;
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

        return (_offset - oldOffset, sizeUsed);
    }

    public long NextValueToEncode => _entries[Math.Min(_offset, _count-1)];

    private int WriteLastBatchAsVarIntDelta(int count, byte* output, int outputSize)
    {
        Debug.Assert(count <= 256);
        var buffer = stackalloc byte[256 * 10]; // 10 bytes per max int64 var int

        var prevIdx = Math.Max(0, _offset - 1);
        var previous = _entries[prevIdx];
        var dest = buffer;
        for (int i = 0; i < count; i++)
        {
            var cur = _entries[i + _offset];
            var delta = cur - previous;
            previous = cur;

            while (delta >= 0x80)
            {
                *dest++ = (byte)(delta | 0x80);
                delta >>= 7;
            }
            *dest++ = (byte)(delta);
        }

        var size = (dest - buffer);
        if (size > outputSize)
            return 0;

        Unsafe.CopyBlock(output, buffer, (uint)size);
        return (int)size;
    }

    private int ProcessBlock(uint* currentEntries)
    {
        var (bestB, maxB, exceptionCount) = FindBestBitWidths(currentEntries);
        _metadata.Add((byte)bestB);
        _metadata.Add((byte)exceptionCount);

        if (exceptionCount > 0)
        {
            _metadata.Add((byte)maxB);
            uint maxVal = 1u << bestB;
            for (int j = 0; j < 256; j++)
            {
                if (currentEntries[j] >= maxVal)
                {
                    var exList = _exceptions[maxB - bestB] ??= new();
                    exList.Add(currentEntries[j] >>> bestB);
                    _metadata.Add((byte)j);
                }
            }
        }

        return bestB * Vector256<byte>.Count;
    }

    private static (int BestBitWidth, int MaxBitWidth, int ExceptionsCount) FindBestBitWidths(uint* entries)
    {
        const int blockSize = 256;
        const int exceptionOverhead = 8;

        var frequencies = stackalloc int[33];
        for (int i = 0; i < blockSize; i++)
        {
            frequencies[32 - BitOperations.LeadingZeroCount(entries[i])]++;
        }
        var bestBitWidth = 32;
        while (frequencies[bestBitWidth] == 0 && bestBitWidth > 0)
        {
            bestBitWidth--;
        }
        var maxBitWidth = bestBitWidth;
        var bestCost = bestBitWidth * blockSize;
        var bestExceptionCount = 0;
        var exceptionsCount = 0;


        for (var curBitWidth = bestBitWidth - 1; curBitWidth >= 0; curBitWidth--)
        {
            var currentExceptions = frequencies[curBitWidth + 1];
            exceptionsCount += currentExceptions;

            var curCost = exceptionsCount * exceptionOverhead +
                          exceptionsCount * (maxBitWidth - curBitWidth) +
                          curBitWidth * blockSize;

            if (curCost < bestCost)
            {
                bestCost = curCost;
                bestBitWidth = curBitWidth;
                bestExceptionCount = exceptionsCount;
            }
        }

        return (bestBitWidth, maxBitWidth, bestExceptionCount);
    }


    public void Dispose()
    {
        NativeMemory.Free(_entriesOutput);
        _entriesOutput = null;
    }
}
