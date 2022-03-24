﻿using System.Globalization;
using System.Net;
using Downloader.Core.Core;
using log4net;

namespace VolatilityAnalyzer
{
    internal class Program
    {
        private static readonly ILog Log = LogManager.GetLogger(typeof(Program));

        private static async Task Main()
        {
            ServicePointManager.DefaultConnectionLimit = 40;
            ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;

            Log.Info("Starting");

            int lookBackDays = -30;
            var downloader = new DefaultDownloader(new NullProgress());
            var range = DateTimeRange.FromDiff(new DateTime(2022, 1, 2, 0, 0, 0, DateTimeKind.Utc),
                TimeSpan.FromDays(lookBackDays));

            const string exchange = "Ftx";

            var currencyPreference = new[] { "USD" };
            var symbols = await downloader.GetSymbols(exchange);

            var filtered = symbols
                .Where(x => !x.Asset.EndsWith("3S") && !x.Asset.EndsWith("3L"))
                .Select(x => new { Symbol = x, Order = Array.IndexOf(currencyPreference, x.Currency) })
                .Where(x => x.Order >= 0)
                .GroupBy(x => x.Symbol.Asset)
                .Select(x => x.OrderBy(s => s.Order).First().Symbol)
                .ToList();

            await using var master = File.CreateText($"{exchange}-summary.csv");
            await master.WriteLineAsync("Asset,Currency,Stddev,Avg,DStddev,DAvg");


            var files = filtered.Select(x => new
                {
                    Filename = downloader.Download(new DownloadTask($"data\\{exchange}", exchange, x.Symbol, range)),
                    SymbolInfo = x
                })
                .ToList();

            using var semaphore = new SemaphoreSlim(1);
            var minPrices = (range.End - range.Start).TotalMinutes * 0.7;

            await Parallel.ForEachAsync(files, async (context, token) =>
            {
                var filename = context.Filename;
                var symbolInfo = context.SymbolInfo;
                Console.WriteLine($"Processing: {symbolInfo}");

                var window = (int)TimeSpan.FromDays(1).TotalMinutes;
                var halfWindow = window / 2;

                var prices = (await File.ReadAllLinesAsync(filename, token)).Select(x => double.Parse(x, NumberStyles.AllowDecimalPoint, CultureInfo.InvariantCulture)).ToList();
                if (prices.Count <= window || prices.Count < minPrices) return;

                //var ma = GetMovingAverages(prices, window);
                var oscilation = GetOscilation(prices);

                //var prevPerc = 0d;

                var stats = prices
                    .Skip(halfWindow)
                    .Take(prices.Count - halfWindow)
                    .Zip(oscilation, (price, oscilation) =>
                    {
                        //var perc = Math.Abs(price == 0 ? 0 : (ma - price) / price * 100);
                        //if (perc > 100) perc *= 0.1d;
                        //var diff = Math.Abs(prevPerc - perc);
                        //prevPerc = perc;
                        return new { Price = price, Oscilation = oscilation };
                    })
                    .ToList();

                var lines = stats.Select(x =>
                    $"{x.Price.Ts()},{x.Oscilation.Ts()}");

                await File.WriteAllLinesAsync(Path.ChangeExtension(filename, ".analyzed.csv"), lines, token);
                //var percStat = stats.Select(x => x.Perc).Stat();
                //var diffStat = stats.Select(x => x.Diff).Stat();
                var oscilationStat = stats.Select(x => x.Oscilation).Stat();

                await semaphore.WaitAsync(token);
                await master.WriteLineAsync($"{symbolInfo.Asset},{symbolInfo.Currency},{oscilationStat}");
                await master.FlushAsync();
                semaphore.Release();
            });
        }

        private static IEnumerable<double> GetOscilation(
            IEnumerable<double> data
        )
        {
            var lastSgn = 0;
            var lastPrice = 0d;
            int changedDirectionCount = 0;

            foreach (var price in data)
            {
                var currentSgn = Math.Sign(lastPrice - price);
                if (currentSgn == 0) continue; // you might want to solve this differently in case if the price is same 
                if (currentSgn != lastSgn)
                {
                    //someUnknowParamThatIdontUnderstand = //... calculate magic parameter based on the fact the direction changed and streak contains consecutive number of fall / raise ticks
                    changedDirectionCount++;
                }
                lastSgn = currentSgn;
                lastPrice = price;
            }

            yield return changedDirectionCount;
        }

        private static IEnumerable<double> GetMovingAverages(
            IEnumerable<double> data,
            int window
        )
        {
            var samples = new Queue<double>();
            var runningTotal = 0d;

            foreach (var price in data)
            {
                samples.Enqueue(price);
                runningTotal += price;

                if (samples.Count <= window) continue;

                runningTotal -= samples.Dequeue();
                yield return runningTotal / samples.Count;
            }
        }
    }

    public static class ExtensionClass
    {
        public static (double Stddev, double Average) Stat(this IEnumerable<double> sequence)
        {
            var values = sequence.ToList();
            var average = values.Average();
            var sum = values.Sum(d => Math.Pow(d - average, 2));
            return new ValueTuple<double, double>(Math.Sqrt(sum / (values.Count - 1)), average);
        }

        public static string Ts(this double value)
        {
            return value.ToString(CultureInfo.InvariantCulture);
        }
    }
}