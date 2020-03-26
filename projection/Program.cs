using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Net.Mime;
using System.Threading;
using System.Threading.Tasks;
using Dapper;
using Newtonsoft.Json;
using SqlStreamStore;
using SqlStreamStore.Streams;
using sqlstreamstore_tests;

namespace projection
{
    /// CREATE TABLE Ledger (
    ///     Name varchar(255),
    ///     Position bigint,
    ///     PRIMARY KEY (Name)
    /// )
    public class Committer
    {
        public class LedgerEntry
        {
            public string Name { get; set; }
            public long Position { get; set; }
        }

        private readonly string _name;
        private readonly string _connectionString;

        public Committer(string name, string connectionString)
        {
            _name = name;
            _connectionString = connectionString;
        }

        public long Init()
        {
            long position = 0;

            using (var connection = new SqlConnection(_connectionString))
            {
                connection.Execute(
                    @"
                        IF NOT EXISTS (SELECT * FROM Ledger WHERE Name = @name)
                            INSERT INTO Ledger (Name, Position)
                            VALUES (@name, @position)
                    ",
                    new { name = _name, position = 0 }
                );

                position = connection
                    .Query<LedgerEntry>(
                        "SELECT * FROM Ledger WHERE Name = @name",
                        new {name = _name})
                    .First()
                    .Position;
            }

            return position;
        }

        public void CommitPosition(long position)
        {
            using (var connection = new SqlConnection(_connectionString))
            {
                connection.Execute(
                    "UPDATE Ledger SET Position = @position WHERE Name = @name;",
                    new { position, name = _name }
                );
            }
        }
    }

    public class Projection
    {
        private readonly MsSqlStreamStore _store;
        private readonly Committer _committer;

        public Projection()
        {
            var connString = "Server=localhost;Database=eqx;User Id=sa;Password=...;";
            _store =  new MsSqlStreamStore(new MsSqlStreamStoreSettings(connString));
            _committer = new Committer("MyProjection", connString);
        }

        private async Task StreamMessageReceived(IAllStreamSubscription subscription, StreamMessage streamMessage,
            CancellationToken cancellationToken)
        {
            try
            {
                var message = await streamMessage.GetJsonData(cancellationToken);

                Console.WriteLine(
                    $"Start processing: " +
                    $"type:{streamMessage.Type}, " +
                    $"positon:{streamMessage.Position}, " +
                    $"stream version:{streamMessage.StreamVersion}, " +
                    $"{message}");

                Thread.Sleep(1000);

                Console.WriteLine("Done");

                _committer.CommitPosition(streamMessage.Position);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }

        }

        private void HasCaughtUp(bool hasCaughtUp)
        {
            Console.WriteLine($"Has caught up {hasCaughtUp}");
        }

        public void Start()
        {
            var position = _committer.Init();
            _store.SubscribeToAll(position, StreamMessageReceived, hasCaughtUp: HasCaughtUp);
        }
    }

    class Program
    {
        static void Main(string[] args)
        {
            var proj = new Projection();
            proj.Start();

            Console.WriteLine("Press any key to stop.");
            Console.ReadLine();
        }
    }
}