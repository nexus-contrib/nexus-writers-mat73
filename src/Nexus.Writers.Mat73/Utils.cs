﻿namespace Nexus.Writers;

public static class Utils
{
    public static (uint, ulong) CalculateChunkParameters(ulong totalLength)
    {
        var maxChunkLength = 4096UL;
        var chunkLength = FindLargestDivisor(totalLength, maxChunkLength);
        var chunkCount = totalLength / chunkLength;

        return ((uint)chunkLength, chunkCount);
    }

    private static ulong FindLargestDivisor(ulong length, ulong limit)
    {
        if (length < limit)
            return length;

        var primes = Factorize(length)
            .GroupBy(value => value)
            .Select(group => group.Aggregate(1UL, (previous, next) => previous * next))
            .Where(prime => prime <= limit)
            .ToList();

        var products = Powerset(primes)
            .Select(combo => combo.Aggregate(1UL, (previous, next) => previous * next))
            .ToList();

        products.Sort();

        var result = products.LastOrDefault(value => value <= limit);

        return result;
    }

    private static List<ulong> Factorize(ulong number)
    {
        var primes = new List<ulong>();

        for (ulong div = 2; number > 1; div++)
        {
            if (number % div == 0)
            {
                while (number % div == 0)
                {
                    number /= div;
                    primes.Add(div);
                }
            }
        }

        return primes;
    }

    private static List<List<T>> Powerset<T>(List<T> list)
    {
        var comboCount = (int)Math.Pow(2, list.Count) - 1;
        var result = new List<List<T>>();

        for (var i = 1; i < comboCount + 1; i++)
        {
            result.Add([]);

            for (var j = 0; j < list.Count; j++)
            {
                if ((i >> j) % 2 != 0)
                    result.Last().Add(list[j]);
            }
        }

        return result;
    }
}