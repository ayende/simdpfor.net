using System.Data;
using System.Windows.Markup;

var list = File.ReadLines(@"C:\Users\oren\Downloads\976504836.txt").Select(long.Parse).ToArray();
unsafe
{
    fixed (long* l = list)
    {
        var fpor = new FastPForEncoder();
        fpor.Init(l, list.Length);

        Console.WriteLine(fpor.Encode());

        var b = new byte[1024];

        var output = new long[1024];
        fixed (long* o = output)
        fixed (byte* p = b)
        {
            var a = fpor.Write(p, 1024);

            var decoder = new FastPForDecoder(p, a.SizeUsed);
            var read = decoder.Read(o, 1024);
            Console.WriteLine(read);
            var idx = 0;
            for (int i = 0; i < read; i++)
            {
                if (list[idx++] != output[i])
                {
                    Console.WriteLine("Bad " + i);
                    return;
                }
            }
            Console.WriteLine(list.Take(1024).SequenceEqual(output));
           
        }

    }
}
