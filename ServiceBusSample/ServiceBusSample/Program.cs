using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using VSCM.MessagingProtocol;
using VSCM.ServiceBusWriter;

namespace ServiceBusSample
{
    class Program
    {
        static void Main(string[] args)
        {
            var app = new Sample();

            app.Run();
        }
    }

    class Sample
    {
        public void Run()
        {
            var results = Menu();

            var file = results.Item1;
            var path = results.Item2;
            var transferqueue = results.Item3;

            Console.Clear();
            Console.WriteLine("Writing contents of " + file + " to " + path + " via " + transferqueue);

            WriteToServiceBus(file, path, transferqueue);

            Console.WriteLine("Press any key to exit");
            Console.ReadLine();
        }

        private Tuple<string, string, string> Menu()
        {
            var file = FileMenu();
            var path = PathMenu();
            var transferqueue = TransferMenu();

            return new Tuple<string, string, string>(file, path, transferqueue);
        }

        private string FileMenu()
        {
            return "sample.csv";
        }

        private string PathMenu()
        {
            Console.Clear();
            Console.WriteLine("Enter a path to write to:");
            Console.WriteLine();

            var pathInput = Console.ReadLine();

            if (!string.IsNullOrEmpty(pathInput))
            {
                return pathInput;
            }

            return PathMenu();
        }

        private string TransferMenu()
        {
            Console.Clear();
            Console.WriteLine("Enter a transfer queue to write to:");
            Console.WriteLine();

            var transferInput = Console.ReadLine();

            if (!string.IsNullOrEmpty(transferInput))
            {
                return transferInput;
            }

            return TransferMenu();
        }

        private void WriteToServiceBus(string file, string path, string transferqueue)
        {
            var lines = File.ReadAllLines("../../Sample Files/" + file);

            var batchId = Guid.NewGuid().ToString();

            var messages = lines.Select(l => new VSCM_ServiceBusMessage { Template = "1-Sample", BatchId = batchId, Message = l }).ToList();

            Stopwatch sw = new Stopwatch();
            sw.Start();

            var errorMessages = new List<ErrorMessage>();

            using (var writer = new VSCM_ServiceBusWriter())
            {
                errorMessages.AddRange(writer.WriteMessagesToServiceBus(path, transferqueue, messages));
            }

            sw.Stop();

            Console.WriteLine("Error Count: " + errorMessages.Count);

            Console.WriteLine("Total Lines Written: " + messages.Count);
            Console.WriteLine("Elapsed time: " + sw.Elapsed.ToString("mm':'ss'.'fff"));
            Console.WriteLine("Message/sec: " + messages.Count / sw.Elapsed.TotalSeconds);
        }
    }
}