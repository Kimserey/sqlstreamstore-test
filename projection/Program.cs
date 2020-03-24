using System;
using System.Threading;
using SqlStreamStore;
using SqlStreamStore.Streams;
using sqlstreamstore_tests;

namespace projection
{
    class Program
    {
        static void Main(string[] args)
        {
            var streamId = new StreamId("Account:93aad7b7-f6d2-438f-911b-8eaba5695d48");
            var store = new MsSqlStreamStore(new MsSqlStreamStoreSettings("Server=localhost;Database=sqlstream;User Id=sa;Password=p@ssw0rd;"));
            var balanceProjection = new BalanceProjection(store, streamId);

            while (true)
            {
                Thread.Sleep(1000);
            }

            Console.WriteLine("Hello World!");
        }
    }
}