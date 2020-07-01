using System;
using System.Data.SqlTypes;

public class MathFunctions
{
    /// <summary>
    /// Caculate Levenshtein distance (based off: https://gist.github.com/Davidblkx/e12ab0bb2aff7fd8072632b396538560)
    /// </summary>
    /// <param name="source">source word</param>
    /// <param name="target">target word</param>
    /// <returns>numeric distance between words</returns>
    [Microsoft.SqlServer.Server.SqlFunction]
    public static SqlInt32 Distance(SqlString source, SqlString target)
    {
        string sourceStr = source.ToString();
        string targetStr = target.ToString();
        int sourceLength = sourceStr.Length;
        int targetLength = targetStr.Length;

        int[,] matrix = new int[sourceLength + 1, targetLength + 1];

        if (sourceLength == 0) return targetLength;
        if (targetLength == 0) return sourceLength;

        for (int i = 0; i <= sourceLength; matrix[i, 0] = i++) ;
        for (int j = 0; j <= targetLength; matrix[0, j] = j++) ;

        for (int i = 1; i <= sourceLength; i++)
        {
            for (int j = 1; j <= targetLength; j++)
            {
                int cost = (sourceStr[i - 1] == targetStr[j - 1]) ? 0 : 1;
                matrix[i, j] = Math.Min(Math.Min(matrix[i - 1, j] + 1, matrix[i, j - 1] + 1), matrix[i - 1, j - 1] + cost);
            }
        }
        return matrix[sourceLength, targetLength];
    }
}
