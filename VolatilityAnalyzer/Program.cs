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
            ServicePointManager.DefaultConnectionLimit = 40;
            ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;

            Log.Info("Starting");

            int lookBackDays = -30;

            var downloader = new DefaultDownloader(new NullProgress());
            var range = DateTimeRange.FromDiff(new DateTime(2022, 1, 2, 0, 0, 0, DateTimeKind.Utc),
                TimeSpan.FromDays(lookBackDays));

            const string exchange = "Ftx";

            var currencyPreference = new[] { "BTC" };
            var symbols = await downloader.GetSymbols(exchange);

            var filtered = symbols
                .Where(x => !x.Asset.EndsWith("3S") && !x.Asset.EndsWith("3L"))
                .Select(x => new { Symbol = x, Order = Array.IndexOf(currencyPreference, x.Currency) })
                .Where(x => x.Order >= 0)
                .GroupBy(x => x.Symbol.Asset)
                .Select(x => x.OrderBy(s => s.Order).First().Symbol)
                .ToList();

            await using var master = File.CreateText($"{exchange}-summary.csv");
            await master.WriteLineAsync("Asset,Currency,Oscilation");

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

                var prices = (await File.ReadAllLinesAsync(filename, token))
                    .Select(x => double.Parse(x, CultureInfo.InvariantCulture))
                    .ToList();

                if (prices.Count <= 1) return;

                var oscilation = GetOscilation(prices);

                await semaphore.WaitAsync(token);
                await master.WriteLineAsync($"{symbolInfo.Asset},{symbolInfo.Currency},{oscilation}");
                await master.FlushAsync();
                semaphore.Release();
            });
        }

        
        private static double GetOscilation(
            List<double> data
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

            return changedDirectionCount;
        }

        private static double GetMa(
            List<double> data
        )
        {
            //dopsat.
            return 0;
        }
    }
}