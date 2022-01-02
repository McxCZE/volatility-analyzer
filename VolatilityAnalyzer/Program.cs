using System.Globalization;
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
            ServicePointManager.DefaultConnectionLimit = 10;
            ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;

            Log.Info("Starting");

            var downloader = new DefaultDownloader(new NullProgress());
            var range = DateTimeRange.FromUtcToday(TimeSpan.FromDays(-365));

            const string exchange = "KUCOIN";

            var currencyPreference = new[] { "USD", "EUR", "USDT" };
            var symbols = await downloader.GetSymbols(exchange);

            var filtered = symbols
                .Select(x => new { Symbol = x, Order = Array.IndexOf(currencyPreference, x.Currency) })
                .Where(x => x.Order >= 0)
                .GroupBy(x => x.Symbol.Asset)
                .Select(x => x.OrderBy(s => s.Order).First().Symbol)
                .ToList();

            await using var master = File.CreateText($"{exchange}-summary.csv");
            await master.WriteLineAsync("Asset,Currency,Stddev,Avg");

            foreach (var symbolInfo in filtered)
            {
                var filename = downloader.Download(new DownloadTask($"data\\{exchange}", exchange, symbolInfo.Symbol, range));

                var window = (int)TimeSpan.FromDays(1).TotalMinutes;
                var halfWindow = window / 2;

                var prices = (await File.ReadAllLinesAsync(filename)).Select(x => double.Parse(x, NumberStyles.AllowDecimalPoint, CultureInfo.InvariantCulture)).ToList();
                var ma = GetMovingAverages(prices, window);

                var stats = prices
                    .Skip(halfWindow)
                    .Take(prices.Count - halfWindow)
                    .Zip(ma, (price, ma) => new { Price = price, MA = ma, Perc = Math.Abs(price == 0 ? 0 : (ma - price) / price * 100) })
                    .ToList();

                var lines = stats.Select(x =>
                    $"{x.Price.Ts()},{x.MA.Ts()},{x.Perc.Ts()}");

                await File.WriteAllLinesAsync(Path.ChangeExtension(filename, ".analyzed.csv"), lines);
                await master.WriteLineAsync($"{symbolInfo.Asset},{symbolInfo.Currency},{stats.Select(x => x.Perc).Stddev().Ts()},{stats.Select(x => x.Perc).Average().Ts()}");
                await master.FlushAsync();
            }
        }

        private static IEnumerable<double> GetMovingAverages(IEnumerable<double> data, int window)
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
        public static double Stddev(this IEnumerable<double> sequence)
        {
            var values = sequence.ToList();
            var average = values.Average();
            var sum = values.Sum(d => Math.Pow(d - average, 2));
            return Math.Sqrt((sum) / (values.Count() - 1));
        }

        public static string Ts(this double value)
        {
            return value.ToString(CultureInfo.InvariantCulture);
        }
    }
}