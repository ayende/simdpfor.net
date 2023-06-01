using System.Runtime.Intrinsics;

namespace Voron.Util.Simd;

public struct MaskEntries : ISimdTransform
{
    Vector256<uint> _mask;

    public MaskEntries(uint mask)
    {
        _mask = Vector256.Create(mask);
    }
    public Vector256<uint> Decode(Vector256<uint> curr, ref Vector256<uint> prev)
    {
        return curr;
    }

    public Vector256<uint> Encode(Vector256<uint> curr, ref Vector256<uint> prev)
    {
        return curr & _mask;
    }
}
