using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

namespace SimdPFor;



public unsafe struct FastPForBufferedReader : IDisposable
{
    private long* _buffer;
    private int _bufferIdx, _usedBuffer;
    public FastPForDecoder Decoder;
    private const int InternalBufferSize = 256;

    public bool IsValid => Decoder.IsValid;

    public FastPForBufferedReader(byte* p, int len)
    {
        Decoder = len > 0 ? new FastPForDecoder(p, len) : default;
        _buffer = null;
        _usedBuffer = 0;
        _bufferIdx = 0;
    }

    public int Fill(long* matches, int count)
    {
        while (Decoder.IsValid)
        {
            if (_bufferIdx != _usedBuffer)
            {
                var read = Math.Min(count, _usedBuffer - _bufferIdx);
                Unsafe.CopyBlock(matches, _buffer + _bufferIdx, (uint)(read * sizeof(long)));

                _bufferIdx += read;
                return read;
            }

            if (count < InternalBufferSize)
            {
                if (_buffer == null)
                {
                    _buffer = (long*)NativeMemory.Alloc(InternalBufferSize * sizeof(long));
                }

                _bufferIdx = 0;
                _usedBuffer = Decoder.Read(_buffer, InternalBufferSize);
                if (_usedBuffer == 0)
                    return 0;
                continue;
            }

            var sizeAligned = count & ~255;
            return Decoder.Read(matches, sizeAligned);
        }

        return 0;
    }

    public void Dispose()
    {
        NativeMemory.Free(_buffer);
        _buffer = null;
        Decoder.Dispose();
    }
}
