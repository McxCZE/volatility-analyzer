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

            var t = Array.IndexOf(currencyPreference, "USDs");

            var filtered = symbols
                .Select(x => new {Symbol = x, Order = Array.IndexOf(currencyPreference, x.Currency)})
                .Where(x => x.Order >= 0)
                .GroupBy(x => x.Symbol.Asset)
                .Select(x => x.OrderBy(s => s.Order).First().Symbol)
                .ToList();

            foreach (var symbolInfo in filtered)
            {
                var filename = downloader.Download(new DownloadTask($"data\\{exchange}", exchange, symbolInfo.Symbol, range));
            }

            //TODO: analyze all pairs
        }
    }
}