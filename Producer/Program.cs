// See https://aka.ms/new-console-template for more information
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Kafka.Public;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace Test
{
    internal class Program
    {
        const string server = "172.17.224.212:9093";
        static string topic;
        static IProducer<Null, string> _producer;
        static async Task SendWithPartition(string? angka)
        {
            List<PartitionMetadata> partitions;
            using (var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = server }).Build())
            {
                var meta = adminClient.GetMetadata(TimeSpan.FromSeconds(20));

                var currentTopic = meta.Topics.SingleOrDefault(t => t.Topic == topic);
                if (currentTopic == null)
                {
                    await adminClient.CreateTopicsAsync(new TopicSpecification[] {
                    new TopicSpecification { Name = topic, ReplicationFactor = 1, NumPartitions = 3 } });
                    meta = adminClient.GetMetadata(TimeSpan.FromSeconds(20));
                    currentTopic = meta.Topics.SingleOrDefault(t => t.Topic == topic);
                }
                partitions = currentTopic.Partitions;
                foreach (var partition in partitions)
                {
                    var result = await _producer.ProduceAsync(new TopicPartition(topic, new Partition(partition.PartitionId)), new Message<Null, string>()
                    {
                        Value = angka
                    });
                    Console.WriteLine(result.Status);
                }

            }
        }

        static async Task SendWithouPartition(string? angka)
        {
            try
            {
                var result = await _producer.ProduceAsync(topic, new Message<Null, string>()
                {
                    Value = angka
                });
                Console.WriteLine(result.Status);

            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }

        }
        static async Task Main(string[] args)
        {

            Console.Write("Masukan nama topic : ");
            topic = Console.ReadLine();
            var config = new ProducerConfig()
            {
                BootstrapServers = server,
                MessageSendMaxRetries = 1,
                RetryBackoffMs = 1000,
                Acks = Acks.All,
                RequestTimeoutMs = 2000,
                SocketTimeoutMs = 2500,
                TransactionTimeoutMs = 1500,
                MaxInFlight = 1,
                LingerMs = 5,
                EnableDeliveryReports = true,
                EnableIdempotence = false
            };
            _producer = new ProducerBuilder<Null, string>(config).Build();
            while (true)
            {
                Console.Write("Masukan angka yang akan di kirim : ");
                var angka = Console.ReadLine();
                if (angka.StartsWith("p"))
                    await SendWithPartition(angka.Substring(1));
                else
                    await SendWithouPartition(angka);


            }
        }
    }
}
