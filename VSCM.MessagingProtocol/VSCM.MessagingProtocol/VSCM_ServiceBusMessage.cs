namespace VSCM.MessagingProtocol
{
    public class VSCM_ServiceBusMessage
    {
        public string Template { get; set; }
        public string BatchId { get; set; }
        public string Message { get; set; }
    }
}