using System;
using System.Text.Json;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;

namespace ServicioBUS
{
    public class Program
    {
        private const string connectionString = "Endpoint=sb://alex384.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=kqnh4PlJi96c5068DpDt8kHuKDbC96h3x+ASbHFH+Bk=";
        private const string incidentsQueueName = "alexcola384";
        private const string ticketsQueueName = "tickets";
        public static async Task Main(string[] args)
        {
            string userMessage = "";
            do
            {
                Console.WriteLine("Por favor, escribe el mensaje que deseas enviar:");
                userMessage = Console.ReadLine();
            }
            while (string.IsNullOrEmpty(userMessage));

            await SendAlarmEventAsync(new AlarmEvent
            {
                Id = Guid.NewGuid().ToString(),
                Message = userMessage,
                Timestamp = DateTime.UtcNow
            });

            await ProcessAlarmEventsAsync();
        }


        private static async Task SendAlarmEventAsync(AlarmEvent alarmEvent)
        {
            ServiceBusClient client = new ServiceBusClient(connectionString);
            try
            {
                ServiceBusSender sender = client.CreateSender(incidentsQueueName);

                string messageBody = JsonSerializer.Serialize(alarmEvent);
                ServiceBusMessage message = new ServiceBusMessage(messageBody);

                await sender.SendMessageAsync(message);

                Console.WriteLine($"Enviado un mensaje a la cola: {incidentsQueueName}");
            }
            finally
            {
                await client.DisposeAsync();
            }
        }

        private static async Task ProcessAlarmEventsAsync()
        {
            ServiceBusClient client = new ServiceBusClient(connectionString);
            try
            {
                ServiceBusProcessor processor = client.CreateProcessor(incidentsQueueName, new ServiceBusProcessorOptions());

                processor.ProcessMessageAsync += MessageHandler;
                processor.ProcessErrorAsync += ErrorHandler;

                await processor.StartProcessingAsync();

                Console.WriteLine("Presione cualquier tecla para finalizar el procesamiento");
                Console.ReadKey();

                await processor.StopProcessingAsync();
            }
            finally
            {
                await client.DisposeAsync();
            }
        }

        private static async Task MessageHandler(ProcessMessageEventArgs args)
        {
            string body = args.Message.Body.ToString();
            AlarmEvent alarmEvent = JsonSerializer.Deserialize<AlarmEvent>(body);

            string ticketId = Guid.NewGuid().ToString();
            Ticket ticket = new Ticket
            {
                TicketId = ticketId,
                AlarmEventId = alarmEvent.Id
            };

            await SendTicketIdAsync(ticket);

            Console.WriteLine($"Procesado el evento de alarma: {alarmEvent.Id} y creado el ticket: {ticketId}");

            await args.CompleteMessageAsync(args.Message);
        }

        private static Task ErrorHandler(ProcessErrorEventArgs args)
        {
            Console.WriteLine($"El manejador de mensajes encontró una excepción {args.Exception}.");
            return Task.CompletedTask;
        }

        private static async Task SendTicketIdAsync(Ticket ticket)
        {
            ServiceBusClient client = new ServiceBusClient(connectionString);
            try
            {
                ServiceBusSender sender = client.CreateSender(ticketsQueueName);

                string messageBody = JsonSerializer.Serialize(ticket);
                ServiceBusMessage message = new ServiceBusMessage(messageBody);

                await sender.SendMessageAsync(message);

                Console.WriteLine($"Enviado ID del ticket a la cola: {ticketsQueueName}");
            }
            finally
            {
                await client.DisposeAsync();
            }
        }
    }
}
