using System.Runtime.InteropServices;

namespace SimdPFor;

[StructLayout(LayoutKind.Sequential, Pack = 1)]
public struct PForHeader
{
    public long Baseline;
    public uint ExceptionsBitmap;
    public ushort MetadataOffset;
    public ushort ExceptionsOffset;
}