// See https://aka.ms/new-console-template for more information
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Kafka.Public;
using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace Test
{
    internal class Program
    {
        static async Task Main(string[] args)
        {

            ClusterClient _cluster;
            IProducer<Null, string> _producer;
            const string server = "172.17.224.212:9093";
            Console.Write("Masukan nama topic : ");
            var topic = Console.ReadLine();
            string _topic = topic;

            var config = new ConsumerConfig
            {
                BootstrapServers = server,
                AutoOffsetReset = AutoOffsetReset.Latest,
                EnableAutoCommit = false,
                EnableAutoOffsetStore = false,
                GroupId = "foo"
            };
            PartitionMetadata partitions;
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
                partitions = currentTopic.Partitions?.LastOrDefault();

            }
            var _consumer = new ConsumerBuilder<string, string>(config).Build();
            //_consumer.Subscribe(_topic);
            Partition p = new Partition(partitions.PartitionId);
            TopicPartition tp = new TopicPartition(topic, p);
            var offset = 0;
            if (File.Exists("latest"))
            {
                offset = Convert.ToInt32(File.ReadAllText("latest")) + 1;
            }
            Confluent.Kafka.Offset o = new Confluent.Kafka.Offset(offset);
            TopicPartitionOffset tpo = new TopicPartitionOffset(tp, o);
            _consumer.Subscribe(topic);
            //_consumer.Assign(tpo);
            var cancelled = false;
            while (!cancelled)
            {
                try
                {
                    var consumeResult = _consumer.Consume();
                    Console.WriteLine($"{consumeResult.Offset} Received : {consumeResult.Value}");
                    if (int.Parse(consumeResult.Value) % 2 == 0)
                    {
                        File.WriteAllText("latest", consumeResult.Offset.ToString());
                    }

                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                }


                // handle consumed message.
            }

            _consumer.Close();
        }
    }
}
