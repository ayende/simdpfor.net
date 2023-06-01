using System.Runtime.InteropServices;

[StructLayout(LayoutKind.Sequential, Pack = 1)]
public struct PForHeader
{
    public long Baseline;
    public uint ExceptionsBitmap;
    public ushort MetadataOffset;
    public ushort ExceptionsOffset;
    public ushort SharedPrefix;
}
