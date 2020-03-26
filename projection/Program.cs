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
    public class Message
    {
        public Guid Id { get; set; }
        public string StreamId { get; set; }
        public long StreamVersion { get; set; }
        public long Position { get; set; }
        public DateTime CreatedUtc { get; set; }
        public string Type { get; set; }
        public string Content { get; set; }
    }

    public interface IConsumer<out TRaw>
    {
        void Start(Func<TRaw, Task> handle);
    }

    public class LedgerEntry
    {
        public string Name { get; set; }
        public long Position { get; set; }
    }

    /// CREATE TABLE Ledger (
    ///     Name varchar(255),
    ///     Position bigint,
    ///     PRIMARY KEY (Name)
    /// )
    public class SqlStreamStoreConsumer : IConsumer<Message>
    {
        private readonly string _name;
        private readonly string _connectionString;
        private readonly MsSqlStreamStore _store;
        private Func<Message, Task> _handle;

        public SqlStreamStoreConsumer(string name, string connectionString)
        {
            _name = name;
            _connectionString = connectionString;
            _store =  new MsSqlStreamStore(new MsSqlStreamStoreSettings(connectionString));
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

        public long GetInitialPosition()
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

        private async Task StreamMessageReceived(IAllStreamSubscription subscription, StreamMessage streamMessage, CancellationToken cancellationToken)
        {
            try
            {
                var message = await streamMessage.GetJsonData(cancellationToken);

                await _handle(new Message
                {
                    Id = streamMessage.MessageId,
                    Position = streamMessage.Position,
                    Type = streamMessage.Type,
                    CreatedUtc = streamMessage.CreatedUtc,
                    StreamId = streamMessage.StreamId,
                    StreamVersion =  streamMessage.StreamVersion,
                    Content = message
                });

                CommitPosition(streamMessage.Position);
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

        public void Start(Func<Message, Task> handle)
        {
            _handle = handle;

            _store.SubscribeToAll(
                GetInitialPosition(),
                StreamMessageReceived,
                hasCaughtUp: HasCaughtUp
            );
        }
    }

    class Program
    {
        static void Main(string[] args)
        {
            IConsumer<Message> proj = new SqlStreamStoreConsumer("MyProjection2", "Server=localhost;Database=eqx;User Id=sa;Password=...");
            proj.Start(msg =>
            {
                Console.WriteLine(
                    $"Start processing: " +
                    $"type:{msg.Type}, " +
                    $"positon:{msg.Position}, " +
                    $"stream version:{msg.StreamVersion}, " +
                    $"{msg.Content}"
                );

                return Task.CompletedTask;
            });

            Console.WriteLine("Press any key to stop.");
            Console.ReadLine();
        }
    }
}