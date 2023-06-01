using System.Diagnostics;
using System.Runtime.Intrinsics;

namespace Voron.Util.Simd;

public unsafe class SimdBitPacking
{
    public static int RequireSizeSegmented(int len, int bits)
    {
        Debug.Assert(bits is >= 0 and <= 32);
        var (full, partial) = Math.DivRem(len * bits, 8);
        return full + (partial > 0 ? 1 : 0);
    }
}
