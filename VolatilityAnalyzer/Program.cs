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
            ServicePointManager.DefaultConnectionLimit = 15;
            ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;

            Log.Info("Starting");

            int lookBackDays = -30;
            var downloader = new DefaultDownloader(new NullProgress());

            var range = DateTimeRange.FromDiff(new DateTime(2022, 1, 2, 0, 0, 0, DateTimeKind.Utc),
                TimeSpan.FromDays(lookBackDays));

            const string exchange = "Ftx";

            var currencyPreference = new[] { "PERP" };
            var symbols = await downloader.GetSymbols(exchange);

            var filtered = symbols
                .Where(x => !x.Asset.EndsWith("3S") && !x.Asset.EndsWith("3L"))
                .Select(x => new { Symbol = x, Order = Array.IndexOf(currencyPreference, x.Currency) })
                .Where(x => x.Order >= 0)
                .GroupBy(x => x.Symbol.Asset)
                .Select(x => x.OrderBy(s => s.Order).First().Symbol)
                .ToList();

            await using var master = File.CreateText($"{exchange}-summary.csv");
            await master.WriteLineAsync("Asset,Currency,PercDiffChange,Oscilation,MagicMM");

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
                var percDiffChange = GetPercDiffChange(prices);
                //var stDeviation = GetStandardDeviation(prices, 15);

                double magic = (percDiffChange + oscilation) / prices.Last();
                
                var asset = symbolInfo.Asset;
                var currency = symbolInfo.Currency;
                var percDiffChangeVal = percDiffChange.ToString().Replace(",", ".");
                var oscilationVal = oscilation;
                var magicVal = magic.ToString().Replace(",", ".");
                var priceLast = prices.Last();

                await semaphore.WaitAsync(token);
                await master.WriteLineAsync($"{asset},{currency},{priceLast},{percDiffChangeVal},{oscilationVal},{magicVal}");
                //Console.WriteLine($"{symbolInfo.Asset},{symbolInfo.Currency},{percDiffChange.ToString().Replace(",", ".")},{oscilation},{pricesDivision.ToString().Replace(",",".")}");
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
                    changedDirectionCount++;
                }
                lastSgn = currentSgn;
                lastPrice = price;
            }

            return changedDirectionCount;
        }

        private static double GetPercDiffChange(
            List<double> data
        )
        {
            var lastSgn = 0;
            var lastPrice = 0d;
            double percDiffChange = 0d;

            foreach (var price in data)
            {
                var currentSgn = Math.Sign(lastPrice - price);
                if (currentSgn == 0) continue; // you might want to solve this differently in case if the price is same 
                if (currentSgn != lastSgn)
                {

                    //someUnknowParamThatIdontUnderstand = //... calculate magic parameter based on the fact the direction changed and streak contains consecutive number of fall / raise ticks
                    percDiffChange += PercentageDifference(lastPrice, price);
                }
                lastSgn = currentSgn;
                lastPrice = price;
            }

            return percDiffChange;
        }

        private static double PercentageDifference(
            double firstValue,
            double secondValue
        )
        {
            double numerator = Math.Abs(firstValue - secondValue);
            double denominator = (firstValue + secondValue) / 2;

            if (numerator != 0)
            {
                double percentageDiff = (numerator / denominator) * 100;
                return percentageDiff;
            }

            return 0;
        }

        private static double GetStandardDeviation(
            List<double> data,
            int smoothMovingAverage
        )
        {
            var sMa = smoothMovingAverage; // in Hours parameter. As to make input easier...
            var smoothMaMinutes = sMa * 60; //14*60=840

            var deviations = 0d;
            int deviationsCount = 0;
            var pricesWindow = 0d;

            int i = 0;
            foreach(var price in data)
            {
                i++; //i = 1 minute;
                pricesWindow += price;
                if (i / smoothMaMinutes % 1 == 0)
                {
                    deviations += pricesWindow / smoothMaMinutes; // 840x{price += price} / 840 = 1,1; (if price, 1,1 all the time)
                    deviationsCount++;
                    //Console.WriteLine(i);
                }
            }

            double result = deviations / deviationsCount; // <- Higher The Number, bigger the oscilation? Need to verify.             
            return result;
        }


    }
}